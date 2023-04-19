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
	options  Options
	filter   filter
	opType   uint8
	result   interface{}
	fieldMap map[string]interface{} // map for updates, sets etc
	qe       QueryExecutor
}

func (o *singleOp) Options() Options {
	return o.options
}

func (o *singleOp) WithOptions(opts Options) Op {
	return &singleOp{
		options:  o.options.Merge(opts),
		filter:   o.filter,
		opType:   o.opType,
		result:   o.result,
		fieldMap: o.fieldMap,
		qe:       o.qe}
}

func (o *singleOp) Add(additions ...Op) Op {
	return multiOp{o}.Add(additions...)
}

func (o *singleOp) Preflight() error {
	return nil
}

func newWriteOp(qe QueryExecutor, f filter, opType uint8, m map[string]interface{}) *singleOp {
	return &singleOp{
		qe:       qe,
		filter:   f,
		opType:   opType,
		fieldMap: m}
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
	mopt := o.filter.table.options.Merge(opt)
	return SelectStatement{
		keyspace:       o.filter.table.keySpace.name,
		table:          o.filter.table.Name(),
		fields:         o.filter.table.generateFieldList(mopt.Select),
		where:          o.filter.relations,
		order:          mopt.ClusteringOrder,
		limit:          mopt.Limit,
		allowFiltering: mopt.AllowFiltering,
		keys:           o.filter.table.info.keys,
	}
}

func (o *singleOp) generateInsert(opt Options) InsertStatement {
	mopt := o.filter.table.options.Merge(opt)
	return InsertStatement{
		keyspace: o.filter.table.keySpace.name,
		table:    o.filter.table.Name(),
		fieldMap: o.fieldMap,
		ttl:      mopt.TTL,
		keys:     o.filter.table.info.keys,
	}
}

func (o *singleOp) generateUpdate(opt Options) UpdateStatement {
	mopt := o.filter.table.options.Merge(opt)
	return UpdateStatement{
		keyspace: o.filter.table.keySpace.name,
		table:    o.filter.table.Name(),
		fieldMap: o.fieldMap,
		where:    o.filter.relations,
		ttl:      mopt.TTL,
		keys:     o.filter.table.info.keys,
	}
}

func (o *singleOp) generateDelete(opt Options) DeleteStatement {
	return DeleteStatement{
		keyspace: o.filter.table.keySpace.name,
		table:    o.filter.table.Name(),
		where:    o.filter.relations,
		keys:     o.filter.table.info.keys,
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
