package gocassa

import (
	"sort"

	"context"
)

const (
	readOpType uint8 = iota
	singleReadOpType
	deleteOpType
	updateOpType
	insertOpType
)

type singleOp struct {
	options Options
	f       filter
	opType  uint8
	result  interface{}
	m       map[string]interface{} // map for updates, sets etc
	qe      QueryExecutor
}

func (o *singleOp) Options() Options {
	return o.options
}

func (o *singleOp) WithOptions(opts Options) Op {
	return &singleOp{
		options: o.options.Merge(opts),
		f:       o.f,
		opType:  o.opType,
		result:  o.result,
		m:       o.m,
		qe:      o.qe}
}

func (o *singleOp) Add(additions ...Op) Op {
	return multiOp{o}.Add(additions...)
}

func (o *singleOp) Preflight() error {
	return nil
}

func newWriteOp(qe QueryExecutor, f filter, opType uint8, m map[string]interface{}) *singleOp {
	return &singleOp{
		qe:     qe,
		f:      f,
		opType: opType,
		m:      m}
}

func (o *singleOp) Run() error {
	switch o.opType {
	case readOpType, singleReadOpType:
		stmt := o.generateSelect(o.options)
		scanner := NewScanner(stmt, o.result)
		return o.qe.QueryWithOptions(o.options, stmt, scanner)
	case insertOpType:
		stmt := o.generateInsert(o.options)
		return o.qe.ExecuteWithOptions(o.options, stmt)
	case updateOpType:
		stmt := o.generateUpdate(o.options)
		return o.qe.ExecuteWithOptions(o.options, stmt)
	case deleteOpType:
		stmt := o.generateDelete(o.options)
		return o.qe.ExecuteWithOptions(o.options, stmt)
	}
	return nil
}

func (o *singleOp) RunWithContext(ctx context.Context) error {
	return o.WithOptions(Options{Context: ctx}).Run()
}

func (o *singleOp) RunAtomically() error {
	return o.Run()
}

func (o *singleOp) RunLoggedBatchWithContext(ctx context.Context) error {
	return o.WithOptions(Options{Context: ctx}).Run()
}

func (o *singleOp) RunAtomicallyWithContext(ctx context.Context) error {
	return o.RunLoggedBatchWithContext(ctx)
}

func (o *singleOp) GenerateStatement() Statement {
	switch o.opType {
	case readOpType, singleReadOpType:
		return o.generateSelect(o.options)
	case insertOpType:
		return o.generateInsert(o.options)
	case updateOpType:
		return o.generateUpdate(o.options)
	case deleteOpType:
		return o.generateDelete(o.options)
	}
	return noOpStatement{}
}

func (o *singleOp) QueryExecutor() QueryExecutor {
	return o.qe
}

func (o *singleOp) generateSelect(opt Options) SelectStatement {
	mopt := o.f.t.options.Merge(opt)
	return SelectStatement{
		keyspace:       o.f.t.keySpace.name,
		table:          o.f.t.Name(),
		fields:         o.f.t.generateFieldList(mopt.Select),
		where:          o.f.rs,
		order:          mopt.ClusteringOrder,
		limit:          mopt.Limit,
		allowFiltering: mopt.AllowFiltering,
		keys:           o.f.t.info.keys,
	}
}

func (o *singleOp) generateInsert(opt Options) InsertStatement {
	mopt := o.f.t.options.Merge(opt)
	return InsertStatement{
		keyspace:    o.f.t.keySpace.name,
		table:       o.f.t.Name(),
		fieldMap:    o.m,
		ttl:         mopt.TTL,
		ifNotExists: mopt.IfNotExists,
		keys:        o.f.t.info.keys,
	}
}

func (o *singleOp) generateUpdate(opt Options) UpdateStatement {
	mopt := o.f.t.options.Merge(opt)
	return UpdateStatement{
		keyspace: o.f.t.keySpace.name,
		table:    o.f.t.Name(),
		fieldMap: o.m,
		where:    o.f.rs,
		ttl:      mopt.TTL,
		keys:     o.f.t.info.keys,
	}
}

func (o *singleOp) generateDelete(opt Options) DeleteStatement {
	return DeleteStatement{
		keyspace: o.f.t.keySpace.name,
		table:    o.f.t.Name(),
		where:    o.f.rs,
		keys:     o.f.t.info.keys,
	}
}

func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
