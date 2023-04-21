package gocassa

import (
	"fmt"
	"time"
)

type multiFlakeSeriesT struct {
	table              Table
	partitionKeyField  string
	clusteringKeyField string
	bucketSize         time.Duration
}

func (o *multiFlakeSeriesT) Table() Table                        { return o.table }
func (o *multiFlakeSeriesT) Create() error                       { return o.Table().Create() }
func (o *multiFlakeSeriesT) CreateIfNotExist() error             { return o.Table().CreateIfNotExist() }
func (o *multiFlakeSeriesT) Name() string                        { return o.Table().Name() }
func (o *multiFlakeSeriesT) Recreate() error                     { return o.Table().Recreate() }
func (o *multiFlakeSeriesT) CreateStatement() (Statement, error) { return o.Table().CreateStatement() }
func (o *multiFlakeSeriesT) CreateIfNotExistStatement() (Statement, error) {
	return o.Table().CreateIfNotExistStatement()
}

func (o *multiFlakeSeriesT) Set(entity interface{}) Op {
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

	return o.Table().
		Set(m)
}

func (o *multiFlakeSeriesT) Update(partitionKey interface{}, flakeID string, m map[string]interface{}) Op {
	timestamp, err := flakeToTime(flakeID)
	if err != nil {
		return errOp{err: err}
	}
	bucket := bucket(timestamp, o.bucketSize)

	return o.Table().
		Where(Eq(o.partitionKeyField, partitionKey),
			Eq(bucketFieldName, bucket),
			Eq(flakeTimestampFieldName, timestamp),
			Eq(o.clusteringKeyField, flakeID)).
		Update(m)
}

func (o *multiFlakeSeriesT) Delete(partitionKey interface{}, flakeID string) Op {
	timestamp, err := flakeToTime(flakeID)
	if err != nil {
		return errOp{err: err}
	}
	bucket := bucket(timestamp, o.bucketSize)

	return o.Table().
		Where(Eq(o.partitionKeyField, partitionKey),
			Eq(bucketFieldName, bucket),
			Eq(flakeTimestampFieldName, timestamp),
			Eq(o.clusteringKeyField, flakeID)).
		Delete()
}

func (o *multiFlakeSeriesT) Read(partitionKey interface{}, flakeID string, pointer interface{}) Op {
	timestamp, err := flakeToTime(flakeID)
	if err != nil {
		return errOp{err: err}
	}
	bucket := bucket(timestamp, o.bucketSize)
	return o.Table().
		Where(Eq(o.partitionKeyField, partitionKey),
			Eq(bucketFieldName, bucket),
			Eq(flakeTimestampFieldName, timestamp),
			Eq(o.clusteringKeyField, flakeID)).
		ReadOne(pointer)
}

func (o *multiFlakeSeriesT) List(partitionKey interface{}, startTime, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := []interface{}{}
	for bucket := o.Buckets(partitionKey, startTime); bucket.Bucket().Before(endTime); bucket = bucket.Next() {
		buckets = append(buckets, bucket.Bucket())
	}
	return o.Table().
		Where(Eq(o.partitionKeyField, partitionKey),
			In(bucketFieldName, buckets...),
			GTE(flakeTimestampFieldName, startTime),
			LT(flakeTimestampFieldName, endTime)).
		Read(pointerToASlice)
}

func (o *multiFlakeSeriesT) Buckets(v interface{}, start time.Time) Buckets {
	return bucketIter{
		current:   start,
		step:      o.bucketSize,
		field:     bucketFieldName,
		invariant: o.Table().Where(Eq(o.partitionKeyField, v))}
}

func (o *multiFlakeSeriesT) ListSince(partitionKey interface{}, flakeID string, window time.Duration, pointerToASlice interface{}) Op {
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
	for bucket := o.Buckets(partitionKey, startTime); bucket.Bucket().Before(endTime); bucket = bucket.Next() {
		buckets = append(buckets, bucket.Bucket())
	}

	return o.Table().
		Where(Eq(o.partitionKeyField, partitionKey),
			In(bucketFieldName, buckets),
			GTE(flakeTimestampFieldName, startTime),
			LT(flakeTimestampFieldName, endTime),
			GT(o.clusteringKeyField, flakeID)).
		Read(pointerToASlice)
}

func (o *multiFlakeSeriesT) WithOptions(opt Options) MultiFlakeSeriesTable {
	return &multiFlakeSeriesT{
		table:              o.Table().WithOptions(opt),
		partitionKeyField:  o.partitionKeyField,
		clusteringKeyField: o.clusteringKeyField,
		bucketSize:         o.bucketSize,
	}
}
