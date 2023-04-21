package gocassa

import (
	"time"
)

type multiTimeSeriesT struct {
	table              Table
	partitionKeyField  string
	timeField          string
	clusteringKeyField string
	bucketSize         time.Duration
}

func (o *multiTimeSeriesT) Table() Table                        { return o.table }
func (o *multiTimeSeriesT) Create() error                       { return o.Table().Create() }
func (o *multiTimeSeriesT) CreateIfNotExist() error             { return o.Table().CreateIfNotExist() }
func (o *multiTimeSeriesT) Name() string                        { return o.Table().Name() }
func (o *multiTimeSeriesT) Recreate() error                     { return o.Table().Recreate() }
func (o *multiTimeSeriesT) CreateStatement() (Statement, error) { return o.Table().CreateStatement() }
func (o *multiTimeSeriesT) CreateIfNotExistStatement() (Statement, error) {
	return o.Table().CreateIfNotExistStatement()
}

func (o *multiTimeSeriesT) Set(entity interface{}) Op {
	m, ok := toMap(entity)
	if !ok {
		panic("Can't set: not able to convert")
	}
	if tim, ok := m[o.timeField].(time.Time); !ok {
		panic("timeField is not actually a time.Time")
	} else {
		m[bucketFieldName] = bucket(tim, o.bucketSize)
	}
	return o.Table().
		Set(m)
}

func (o *multiTimeSeriesT) Update(partitionKey interface{}, timestamp time.Time, clusteringKey interface{}, m map[string]interface{}) Op {
	bucket := bucket(timestamp, o.bucketSize)
	return o.Table().
		Where(Eq(o.partitionKeyField, partitionKey),
			Eq(bucketFieldName, bucket),
			Eq(o.timeField, timestamp),
			Eq(o.clusteringKeyField, clusteringKey)).
		Update(m)
}

func (o *multiTimeSeriesT) Delete(partitionKey interface{}, timestamp time.Time, clusteringKey interface{}) Op {
	bucket := bucket(timestamp, o.bucketSize)
	return o.Table().
		Where(Eq(o.partitionKeyField, partitionKey),
			Eq(bucketFieldName, bucket),
			Eq(o.timeField, timestamp),
			Eq(o.clusteringKeyField, clusteringKey)).
		Delete()
}

func (o *multiTimeSeriesT) Read(partitionKey interface{}, timestamp time.Time, clusteringKey, pointer interface{}) Op {
	bucket := bucket(timestamp, o.bucketSize)
	return o.Table().
		Where(Eq(o.partitionKeyField, partitionKey),
			Eq(bucketFieldName, bucket),
			Eq(o.timeField, timestamp),
			Eq(o.clusteringKeyField, clusteringKey)).
		ReadOne(pointer)

}

func (o *multiTimeSeriesT) List(partitionKey interface{}, startTime time.Time, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := []interface{}{}
	for bucket := o.Buckets(partitionKey, startTime); bucket.Bucket().Before(endTime); bucket = bucket.Next() {
		buckets = append(buckets, bucket.Bucket())
	}
	return o.Table().
		Where(Eq(o.partitionKeyField, partitionKey),
			In(bucketFieldName, buckets...),
			GTE(o.timeField, startTime),
			LTE(o.timeField, endTime)).
		Read(pointerToASlice)
}

func (o *multiTimeSeriesT) Buckets(partitionKey interface{}, start time.Time) Buckets {
	return bucketIter{
		current:   start,
		step:      o.bucketSize,
		field:     bucketFieldName,
		invariant: o.Table().Where(Eq(o.partitionKeyField, partitionKey))}
}

func (o *multiTimeSeriesT) WithOptions(opt Options) MultiTimeSeriesTable {
	return &multiTimeSeriesT{
		table:              o.Table().WithOptions(opt),
		partitionKeyField:  o.partitionKeyField,
		timeField:          o.timeField,
		clusteringKeyField: o.clusteringKeyField,
		bucketSize:         o.bucketSize,
	}
}
