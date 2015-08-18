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
	Table
	idField    string
	bucketSize time.Duration
}

func (o *flakeSeriesT) Set(v interface{}) Op {
	m, ok := toMap(v)
	if !ok {
		panic("Can't set: not able to convert")
	}
	id, ok := m[o.idField].(string)
	if !ok {
		panic(fmt.Sprintf("Id field (%s) is not present or is not a string", o.idField))
	}

	timestamp, err := flakeToTime(id)
	if err != nil {
		return errOp{err: err}
	}

	m[flakeTimestampFieldName] = timestamp
	m[bucketFieldName] = o.bucket(timestamp.Unix())

	return o.Table.Set(m)
}

func (o *flakeSeriesT) Update(id string, m map[string]interface{}) Op {
	timestamp, err := flakeToTime(id)
	if err != nil {
		return errOp{err: err}
	}
	bucket := o.bucket(timestamp.Unix())

	return o.Where(Eq(bucketFieldName, bucket), Eq(flakeTimestampFieldName, timestamp), Eq(o.idField, id)).Update(m)
}

func (o *flakeSeriesT) Delete(id string) Op {
	timestamp, err := flakeToTime(id)
	if err != nil {
		return errOp{err: err}
	}
	bucket := o.bucket(timestamp.Unix())

	return o.Where(Eq(bucketFieldName, bucket), Eq(flakeTimestampFieldName, timestamp), Eq(o.idField, id)).Delete()
}

func (o *flakeSeriesT) Read(id string, pointer interface{}) Op {
	timestamp, err := flakeToTime(id)
	if err != nil {
		return errOp{err: err}
	}
	bucket := o.bucket(timestamp.Unix())
	return o.Where(Eq(bucketFieldName, bucket), Eq(flakeTimestampFieldName, timestamp), Eq(o.idField, id)).ReadOne(pointer)
}

func (o *flakeSeriesT) List(startTime, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := o.buckets(startTime, endTime)
	return o.Where(In(bucketFieldName, buckets...), GTE(flakeTimestampFieldName, startTime), LT(flakeTimestampFieldName, endTime)).Read(pointerToASlice)
}

func (o *flakeSeriesT) ListSince(id string, window time.Duration, pointerToASlice interface{}) Op {
	startTime, err := flakeToTime(id)
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
	buckets := o.buckets(startTime, endTime)
	return o.Where(In(bucketFieldName, buckets), GTE(flakeTimestampFieldName, startTime), LT(flakeTimestampFieldName, endTime), GT(o.idField, id)).Read(pointerToASlice)
}

func (o *flakeSeriesT) WithOptions(opt Options) FlakeSeriesTable {
	return &flakeSeriesT{
		Table:      o.Table.WithOptions(opt),
		bucketSize: o.bucketSize}
}

func (o *flakeSeriesT) buckets(startTime, endTime time.Time) []interface{} {
	buckets := []interface{}{}
	start := o.bucket(startTime.Unix())
	for i := start; i < endTime.Unix()*1000; i += int64(o.bucketSize/time.Second) * 1000 {
		buckets = append(buckets, i)
	}

	return buckets
}

func (o *flakeSeriesT) bucket(secs int64) int64 {
	return (secs - secs%int64(o.bucketSize/time.Second)) * 1000
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
