/*
 *
 * k6 - a next-generation load testing tool
 * Copyright (C) 2016 Load Impact
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package influxdb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	client "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/sirupsen/logrus"
	"go.k6.io/k6/output"
	"go.k6.io/k6/stats"
)

// FieldKind defines Enum for tag-to-field type conversion
type FieldKind int

const (
	// String field (default)
	String FieldKind = iota
	// Int field
	Int
	// Float field
	Float
	// Bool field
	Bool
)

// Output is the influxdb Output struct
type Output struct {
	output.SampleBuffer

	Client client.Client
	Config Config

	params          output.Params
	periodicFlusher *output.PeriodicFlusher
	logger          logrus.FieldLogger
	semaphoreCh     chan struct{}
	fieldKinds      map[string]FieldKind
	pointWriter     api.WriteAPIBlocking
}

// New returns new influxdb output
func New(params output.Params) (output.Output, error) {
	return newOutput(params)
}

func newOutput(params output.Params) (*Output, error) {
	conf, err := GetConsolidatedConfig(params.JSONConfig, params.Environment, params.ConfigArgument)
	if err != nil {
		return nil, err
	}
	if conf.Bucket.String == "" {
		return nil, fmt.Errorf("invalid configuration: a Bucket value is required")
	}
	if conf.ConcurrentWrites.Int64 <= 0 {
		return nil, errors.New("influxdb's ConcurrentWrites must be a positive number")
	}
	cl, err := MakeClient(conf)
	if err != nil {
		return nil, err
	}
	fldKinds, err := MakeFieldKinds(conf)
	return &Output{
		params: params,
		logger: params.Logger.WithFields(logrus.Fields{
			"output": "InfluxDBv2",
		}),
		Client:      cl,
		Config:      conf,
		semaphoreCh: make(chan struct{}, conf.ConcurrentWrites.Int64),
		fieldKinds:  fldKinds,
		pointWriter: cl.WriteAPIBlocking(conf.Organization.String, conf.Bucket.String),
	}, err
}

func (o *Output) extractTagsToValues(tags map[string]string, values map[string]interface{}) map[string]interface{} {
	for tag, kind := range o.fieldKinds {
		if val, ok := tags[tag]; ok {
			var v interface{}
			var err error
			switch kind {
			case String:
				v = val
			case Bool:
				v, err = strconv.ParseBool(val)
			case Float:
				v, err = strconv.ParseFloat(val, 64)
			case Int:
				v, err = strconv.ParseInt(val, 10, 64)
			}
			if err == nil {
				values[tag] = v
			} else {
				values[tag] = val
			}
			delete(tags, tag)
		}
	}
	return values
}

func (o *Output) batchFromSamples(containers []stats.SampleContainer) []*write.Point {
	type cacheItem struct {
		tags   map[string]string
		values map[string]interface{}
	}
	cache := map[*stats.SampleTags]cacheItem{}

	var points []*write.Point
	for _, container := range containers {
		samples := container.GetSamples()
		for _, sample := range samples {
			var tags map[string]string
			values := make(map[string]interface{})
			if cached, ok := cache[sample.Tags]; ok {
				tags = cached.tags
				for k, v := range cached.values {
					values[k] = v
				}
			} else {
				tags = sample.Tags.CloneTags()
				o.extractTagsToValues(tags, values)
				cache[sample.Tags] = cacheItem{tags, values}
			}
			values["value"] = sample.Value
			p := client.NewPoint(
				sample.Metric.Name,
				tags,
				values,
				sample.Time,
			)
			points = append(points, p)
		}
	}

	return points
}

// Description returns a human-readable description of the output.
func (o *Output) Description() string {
	return fmt.Sprintf("InfluxDBv2 (%s)", o.Config.Addr.String)
}

// Start tries to open the specified JSON file and starts the goroutine for
// metric flushing. If gzip encoding is specified, it also handles that.
func (o *Output) Start() error {
	o.logger.Debug("Starting...")

	if o.Config.Organization.String != "" && o.Config.Bucket.String != "" {
		if err := o.createBucket(); err != nil {
			return fmt.Errorf("is not possible to create or find the specified Bucket: %w", err)
		}
	}

	pf, err := output.NewPeriodicFlusher(time.Duration(o.Config.PushInterval.Duration), o.flushMetrics)
	if err != nil {
		return err //nolint:wrapcheck
	}
	o.logger.Debug("Started!")
	o.periodicFlusher = pf

	return nil
}

// Stop flushes any remaining metrics and stops the goroutine.
func (o *Output) Stop() error {
	o.logger.Debug("Stopping...")
	defer o.logger.Debug("Stopped!")
	o.periodicFlusher.Stop()
	o.Client.Close()
	return nil
}

// createBucket creates the configured bucket if it doesn't exist
func (o *Output) createBucket() error {
	ctx := context.Background()

	org, err := o.Client.OrganizationsAPI().FindOrganizationByName(ctx, o.Config.Organization.String)
	if err != nil {
		return err
	}

	buckets := o.Client.BucketsAPI()
	_, err = buckets.FindBucketByName(ctx, o.Config.Bucket.String)
	if err == nil {
		// the bucket already exists
		return nil
	}

	// TODO: can we do a better check?
	if err.Error() != fmt.Sprintf("bucket '%s' not found", o.Config.Bucket.String) {
		return err
	}

	// create a bucket with the default (infinite) retention policy
	_, err = o.Client.BucketsAPI().CreateBucketWithName(ctx, org, o.Config.Bucket.String)
	if err != nil {
		return err
	}

	return nil
}

func (o *Output) flushMetrics() {
	samples := o.GetBufferedSamples()
	if len(samples) == 0 {
		o.logger.Debug("Any buffered samples, skipping the flush operation")
		return
	}

	o.semaphoreCh <- struct{}{}
	defer func() {
		<-o.semaphoreCh
	}()
	o.logger.Debug("Committing...")
	o.logger.WithField("samples", len(samples)).Debug("Writing...")

	batch := o.batchFromSamples(samples)
	o.logger.WithField("points", len(batch)).Debug("Writing...")

	startTime := time.Now()
	if err := o.pointWriter.WritePoint(context.Background(), batch...); err != nil {
		o.logger.WithError(err).Error("Couldn't write stats")
	}
	o.logger.WithField("t", time.Since(startTime)).Debug("Batch written!")
}
