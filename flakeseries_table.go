package gocassa

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mattheath/base62"
	"github.com/mattheath/kala/bigflake"
	"github.com/mattheath/kala/util"
)

// we have to put the timestamp from the flake ID into a field so that we can
// execute queries with it
const flakeTimestampFieldName = "flake_created"

type flakeSeriesT struct {
	table              Table
	clusteringKeyField string
	bucketSize         time.Duration
}

func (o *flakeSeriesT) Table() Table                        { return o.table }
func (o *flakeSeriesT) Create() error                       { return o.Table().Create() }
func (o *flakeSeriesT) CreateIfNotExist() error             { return o.Table().CreateIfNotExist() }
func (o *flakeSeriesT) Name() string                        { return o.Table().Name() }
func (o *flakeSeriesT) Recreate() error                     { return o.Table().Recreate() }
func (o *flakeSeriesT) CreateStatement() (Statement, error) { return o.Table().CreateStatement() }
func (o *flakeSeriesT) CreateIfNotExistStatement() (Statement, error) {
	return o.Table().CreateIfNotExistStatement()
}

func (o *flakeSeriesT) Set(entity interface{}) Op {
	m, ok := toMap(entity)
	if !ok {
		panic("Can't set: not able to convert")
	}
	id, ok := m[o.clusteringKeyField].(string)
	if !ok {
		panic(fmt.Sprintf("Id field (%s) is not present or is not a string", o.clusteringKeyField))
	}

	timestamp, err := flakeToTime(id)
	if err != nil {
		return errOp{err: err}
	}

	m[flakeTimestampFieldName] = timestamp
	m[bucketFieldName] = bucket(timestamp, o.bucketSize)

	return o.Table().Set(m)
}

func (o *flakeSeriesT) Update(flakeID string, m map[string]interface{}) Op {
	timestamp, err := flakeToTime(flakeID)
	if err != nil {
		return errOp{err: err}
	}
	bucket := bucket(timestamp, o.bucketSize)

	return o.Table().
		Where(Eq(bucketFieldName, bucket),
			Eq(flakeTimestampFieldName, timestamp),
			Eq(o.clusteringKeyField, flakeID)).
		Update(m)
}

func (o *flakeSeriesT) Delete(flakeID string) Op {
	timestamp, err := flakeToTime(flakeID)
	if err != nil {
		return errOp{err: err}
	}
	bucket := bucket(timestamp, o.bucketSize)

	return o.Table().
		Where(Eq(bucketFieldName, bucket),
			Eq(flakeTimestampFieldName, timestamp),
			Eq(o.clusteringKeyField, flakeID)).
		Delete()
}

func (o *flakeSeriesT) Read(flakeID string, pointer interface{}) Op {
	timestamp, err := flakeToTime(flakeID)
	if err != nil {
		return errOp{err: err}
	}
	bucket := bucket(timestamp, o.bucketSize)
	return o.Table().
		Where(Eq(bucketFieldName, bucket),
			Eq(flakeTimestampFieldName, timestamp),
			Eq(o.clusteringKeyField, flakeID)).
		ReadOne(pointer)
}

func (o *flakeSeriesT) List(startTime, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := []interface{}{}
	for bucket := o.Buckets(startTime); bucket.Bucket().Before(endTime); bucket = bucket.Next() {
		buckets = append(buckets, bucket.Bucket())
	}
	return o.Table().
		Where(In(bucketFieldName, buckets...),
			GTE(flakeTimestampFieldName, startTime),
			LT(flakeTimestampFieldName, endTime)).
		Read(pointerToASlice)
}

func (o *flakeSeriesT) Buckets(start time.Time) Buckets {
	return bucketIter{
		current:   start,
		step:      o.bucketSize,
		field:     bucketFieldName,
		invariant: o.Table().Where()}
}

func (o *flakeSeriesT) ListSince(flakeID string, window time.Duration, pointerToASlice interface{}) Op {
	startTime, err := flakeToTime(flakeID)
	if err != nil {
		return errOp{err: err}
	}

	var endTime time.Time
	if window == 0 {
		// no window set - so go up until 5 mins in the future
		endTime = time.Now().Add(5 * time.Minute)
	} else {
		endTime = startTime.Add(window)
	}

	buckets := []interface{}{}
	for bucket := o.Buckets(startTime); bucket.Bucket().Before(endTime); bucket = bucket.Next() {
		buckets = append(buckets, bucket.Bucket())
	}

	return o.Table().
		Where(In(bucketFieldName, buckets),
			GTE(flakeTimestampFieldName, startTime),
			LT(flakeTimestampFieldName, endTime),
			GT(o.clusteringKeyField, flakeID)).
		Read(pointerToASlice)
}

func (o *flakeSeriesT) WithOptions(opt Options) FlakeSeriesTable {
	return &flakeSeriesT{
		table:              o.Table().WithOptions(opt),
		clusteringKeyField: o.clusteringKeyField,
		bucketSize:         o.bucketSize}
}

func flakeToTime(id string) (time.Time, error) {
	parts := strings.Split(id, "_")

	if len(parts) < 2 {
		return time.Time{}, errors.New("Invalid flake id")
	}

	intId := base62.DecodeToBigInt(parts[len(parts)-1])

	msTime, _, _ := bigflake.ParseId(intId)
	timestamp := util.MsInt64ToTime(msTime)

	return timestamp, nil
}
