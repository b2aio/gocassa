package gocassa

import (
	"time"
)

type multiKeyTimeSeriesT struct {
	table               Table
	partitionKeyFields  []string
	timeField           string
	clusteringKeyFields []string
	bucketSize          time.Duration
}

func (o *multiKeyTimeSeriesT) Table() Table            { return o.table }
func (o *multiKeyTimeSeriesT) Create() error           { return o.Table().Create() }
func (o *multiKeyTimeSeriesT) CreateIfNotExist() error { return o.Table().CreateIfNotExist() }
func (o *multiKeyTimeSeriesT) Name() string            { return o.Table().Name() }
func (o *multiKeyTimeSeriesT) Recreate() error         { return o.Table().Recreate() }
func (o *multiKeyTimeSeriesT) CreateStatement() (Statement, error) {
	return o.Table().CreateStatement()
}
func (o *multiKeyTimeSeriesT) CreateIfNotExistStatement() (Statement, error) {
	return o.Table().CreateIfNotExistStatement()
}

func (o *multiKeyTimeSeriesT) Set(entity interface{}) Op {
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

func (o *multiKeyTimeSeriesT) Update(partitionKeys map[string]interface{}, timestamp time.Time, clusteringKeys map[string]interface{}, m map[string]interface{}) Op {
	bucket := bucket(timestamp, o.bucketSize)
	relations := make([]Relation, 0)
	relations = append(relations, o.ListOfEqualRelations(partitionKeys, clusteringKeys)...)
	relations = append(relations, Eq(bucketFieldName, bucket))
	relations = append(relations, Eq(o.timeField, timestamp))

	return o.Table().
		Where(relations...).
		Update(m)
}

func (o *multiKeyTimeSeriesT) Delete(partitionKeys map[string]interface{}, timestamp time.Time, clusteringKeys map[string]interface{}) Op {
	bucket := bucket(timestamp, o.bucketSize)
	relations := make([]Relation, 0)
	relations = append(relations, o.ListOfEqualRelations(partitionKeys, clusteringKeys)...)
	relations = append(relations, Eq(bucketFieldName, bucket))
	relations = append(relations, Eq(o.timeField, timestamp))

	return o.Table().
		Where(relations...).
		Delete()
}

func (o *multiKeyTimeSeriesT) Read(partitionKeys map[string]interface{}, timestamp time.Time, clusteringKeys map[string]interface{}, pointer interface{}) Op {
	bucket := bucket(timestamp, o.bucketSize)
	relations := make([]Relation, 0)
	relations = append(relations, o.ListOfEqualRelations(partitionKeys, clusteringKeys)...)
	relations = append(relations, Eq(bucketFieldName, bucket))
	relations = append(relations, Eq(o.timeField, timestamp))

	return o.Table().
		Where(relations...).
		ReadOne(pointer)

}

func (o *multiKeyTimeSeriesT) List(partitionKeys map[string]interface{}, startTime time.Time, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := []interface{}{}
	for bucket := o.Buckets(partitionKeys, startTime); bucket.Bucket().Before(endTime); bucket = bucket.Next() {
		buckets = append(buckets, bucket.Bucket())
	}

	relations := make([]Relation, 0)
	relations = append(relations, o.ListOfEqualRelations(partitionKeys, nil)...)
	relations = append(relations, In(bucketFieldName, buckets...))
	relations = append(relations, GTE(o.timeField, startTime))
	relations = append(relations, LTE(o.timeField, endTime))

	return o.Table().
		Where(relations...).
		Read(pointerToASlice)
}

func (o *multiKeyTimeSeriesT) Buckets(partitionKeys map[string]interface{}, start time.Time) Buckets {
	return bucketIter{
		current:   start,
		step:      o.bucketSize,
		field:     bucketFieldName,
		invariant: o.Table().Where(o.ListOfEqualRelations(partitionKeys, nil)...)}
}

func (o *multiKeyTimeSeriesT) WithOptions(opt Options) MultiKeyTimeSeriesTable {
	return &multiKeyTimeSeriesT{
		table:               o.Table().WithOptions(opt),
		partitionKeyFields:  o.partitionKeyFields,
		timeField:           o.timeField,
		clusteringKeyFields: o.clusteringKeyFields,
		bucketSize:          o.bucketSize,
	}
}

func (o *multiKeyTimeSeriesT) ListOfEqualRelations(partitionKeys, clusteringKeys map[string]interface{}) []Relation {
	relations := make([]Relation, 0)

	for _, field := range o.partitionKeyFields {
		if value := partitionKeys[field]; value != nil && value != "" {
			relation := Eq(field, value)
			relations = append(relations, relation)
		}
	}

	for _, field := range o.clusteringKeyFields {
		if value := clusteringKeys[field]; value != nil && value != "" {
			relation := Eq(field, value)
			relations = append(relations, relation)
		}
	}

	return relations
}
