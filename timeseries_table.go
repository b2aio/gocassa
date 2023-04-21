package gocassa

import (
	"time"
)

const bucketFieldName = "bucket"

type timeSeriesT struct {
	table              Table
	timeField          string
	clusteringKeyField string
	bucketSize         time.Duration
}

func (o *timeSeriesT) Table() Table                        { return o.table }
func (o *timeSeriesT) Create() error                       { return o.Table().Create() }
func (o *timeSeriesT) CreateIfNotExist() error             { return o.Table().CreateIfNotExist() }
func (o *timeSeriesT) Name() string                        { return o.Table().Name() }
func (o *timeSeriesT) Recreate() error                     { return o.Table().Recreate() }
func (o *timeSeriesT) CreateStatement() (Statement, error) { return o.Table().CreateStatement() }
func (o *timeSeriesT) CreateIfNotExistStatement() (Statement, error) {
	return o.Table().CreateIfNotExistStatement()
}

func (o *timeSeriesT) Set(entity interface{}) Op {
	m, ok := toMap(entity)
	if !ok {
		panic("Can't set: not able to convert")
	}
	if tim, ok := m[o.timeField].(time.Time); !ok {
		panic("timeField is not actually a time.Time")
	} else {
		m[bucketFieldName] = bucket(tim, o.bucketSize)
	}
	return o.Table().Set(m)
}

func (o *timeSeriesT) Update(timestamp time.Time, clusteringKey interface{}, m map[string]interface{}) Op {
	bucket := bucket(timestamp, o.bucketSize)
	return o.Table().
		Where(Eq(bucketFieldName, bucket),
			Eq(o.timeField, timestamp),
			Eq(o.clusteringKeyField, clusteringKey)).
		Update(m)
}

func (o *timeSeriesT) Delete(timestamp time.Time, clusteringKey interface{}) Op {
	bucket := bucket(timestamp, o.bucketSize)
	return o.Table().
		Where(Eq(bucketFieldName, bucket),
			Eq(o.timeField, timestamp),
			Eq(o.clusteringKeyField, clusteringKey)).
		Delete()
}

func (o *timeSeriesT) Read(timestamp time.Time, clusteringKey, pointer interface{}) Op {
	bucket := bucket(timestamp, o.bucketSize)
	return o.Table().
		Where(Eq(bucketFieldName, bucket),
			Eq(o.timeField, timestamp),
			Eq(o.clusteringKeyField, clusteringKey)).
		ReadOne(pointer)
}

func (o *timeSeriesT) List(startTime time.Time, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := []interface{}{}
	for bucket := o.Buckets(startTime); bucket.Bucket().Before(endTime); bucket = bucket.Next() {
		buckets = append(buckets, bucket.Bucket())
	}
	return o.Table().
		Where(In(bucketFieldName, buckets...),
			GTE(o.timeField, startTime),
			LTE(o.timeField, endTime)).
		Read(pointerToASlice)
}

func (o *timeSeriesT) Buckets(start time.Time) Buckets {
	return bucketIter{
		timestamp: start,
		step:      o.bucketSize,
		field:     bucketFieldName,
		invariant: o.Table().Where()}
}

func (o *timeSeriesT) WithOptions(opt Options) TimeSeriesTable {
	return &timeSeriesT{
		table:              o.Table().WithOptions(opt),
		timeField:          o.timeField,
		clusteringKeyField: o.clusteringKeyField,
		bucketSize:         o.bucketSize,
	}
}
