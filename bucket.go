package gocassa

import (
	"time"
)

func bucket(timestamp time.Time, step time.Duration) time.Time {
	return bucketIter{current: timestamp, step: step}.Bucket()
}

type bucketIter struct {
	current   time.Time
	step      time.Duration
	field     string
	invariant Filter
}

func (b bucketIter) String() string {
	return b.current.String()
}

func (b bucketIter) Bucket() time.Time {
	step := b.step
	if step < time.Second {
		step = time.Second
	}
	secs := b.current.Unix()
	return time.Unix((secs - secs%int64(step/time.Second)), 0)
}

func (b bucketIter) Next() Buckets {
	return bucketIter{
		current:   b.current.Add(b.step),
		step:      b.step,
		invariant: b.invariant,
		field:     b.field,
	}
}

func (b bucketIter) Prev() Buckets {
	return bucketIter{
		current:   b.current.Add(-b.step),
		step:      b.step,
		invariant: b.invariant,
		field:     b.field,
	}
}

func (b bucketIter) Filter() Filter {
	rels := b.invariant.Relations()
	rels = append(rels, Eq(b.field, b.Bucket()))
	return b.invariant.Table().Where(rels...)
}
