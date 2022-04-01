package gocassa

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"context"

	"github.com/gocql/gocql"
	"github.com/google/btree"
)

// MockKeySpace implements the KeySpace interface and constructs in-memory tables.
type mockKeySpace struct {
	k
}

type mockOp struct {
	options      Options
	funcs        []func(mockOp) error
	preflightErr error
}

func newOp(f func(mockOp) error) mockOp {
	return mockOp{
		funcs: []func(mockOp) error{f},
	}
}

func (m mockOp) Add(ops ...Op) Op {
	return mockMultiOp{m}.Add(ops...)
}

func (m mockOp) Run() error {
	for _, f := range m.funcs {
		err := f(m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m mockOp) RunWithContext(ctx context.Context) error {
	return m.WithOptions(Options{Context: ctx}).Run()
}

func (m mockOp) Options() Options {
	return m.options
}

func (m mockOp) WithOptions(opt Options) Op {
	return mockOp{
		options: m.options.Merge(opt),
		funcs:   m.funcs,
	}
}

func (m mockOp) RunAtomically() error {
	return m.Run()
}

func (m mockOp) RunLoggedBatchWithContext(ctx context.Context) error {
	return m.WithOptions(Options{Context: ctx}).Run()
}

func (m mockOp) RunAtomicallyWithContext(ctx context.Context) error {
	return m.RunLoggedBatchWithContext(ctx)
}

func (m mockOp) GenerateStatement() Statement {
	return noOpStatement{}
}

func (m mockOp) QueryExecutor() QueryExecutor {
	return nil
}

func (m mockOp) Preflight() error {
	return m.preflightErr
}

type mockMultiOp []Op

func (mo mockMultiOp) Run() error {
	if err := mo.Preflight(); err != nil {
		return err
	}
	for i, op := range mo {
		errorInjector := getErrorInjector(op.Options().Context)
		if errToReturn := errorInjector.shouldReturnErr(op, i, len(mo)); errToReturn != nil {
			return errToReturn
		}
		if err := op.Run(); err != nil {
			return err
		}
	}
	return nil
}

func (mo mockMultiOp) RunWithContext(ctx context.Context) error {
	return mo.WithOptions(Options{Context: ctx}).Run()
}

func (mo mockMultiOp) RunAtomically() error {
	return mo.Run()
}

func (mo mockMultiOp) RunLoggedBatchWithContext(ctx context.Context) error {
	return mo.WithOptions(Options{Context: ctx}).Run()
}

func (mo mockMultiOp) RunAtomicallyWithContext(ctx context.Context) error {
	return mo.RunLoggedBatchWithContext(ctx)
}

func (mo mockMultiOp) GenerateStatement() Statement {
	return noOpStatement{}
}

func (mo mockMultiOp) QueryExecutor() QueryExecutor {
	return nil
}

func (mo mockMultiOp) Add(inOps ...Op) Op {
	ops := make(mockMultiOp, 0, len(inOps))
	for _, op := range inOps {
		// If any multiOps were passed, flatten them out
		switch op := op.(type) {
		case mockMultiOp:
			ops = append(ops, op...)
		case mockOp:
			ops = append(ops, op)
		default:
			panic("can't Add non-mock ops to mockMultiOp")
		}
	}
	return append(mo, ops...)
}

func (mo mockMultiOp) Options() Options {
	var opts Options
	for _, op := range mo {
		opts = opts.Merge(op.Options())
	}
	return opts
}

func (mo mockMultiOp) WithOptions(opts Options) Op {
	result := make(mockMultiOp, len(mo))
	for i, op := range mo {
		result[i] = op.WithOptions(opts)
	}
	return result
}

func (mo mockMultiOp) Preflight() error {
	for _, op := range mo {
		if err := op.Preflight(); err != nil {
			return err
		}
	}
	return nil
}

func (ks *mockKeySpace) NewTable(name string, entity interface{}, fieldSource map[string]interface{}, keys Keys) Table {
	mt := &MockTable{
		RWMutex:     &sync.RWMutex{},
		ksName:      ks.Name(),
		tableName:   name,
		entity:      entity,
		keys:        keys,
		fieldSource: fieldSource,
		rows:        map[rowKey]*btree.BTree{},
		mtx:         &sync.RWMutex{},
	}

	fields := []string{}
	for _, k := range sortedKeys(fieldSource) {
		fields = append(fields, k)
	}
	mt.fields = fields

	return mt
}

func NewMockKeySpace() KeySpace {
	ks := &mockKeySpace{}
	ks.tableFactory = ks
	return ks
}

// MockTable implements the Table interface and stores rows in-memory.
type MockTable struct {
	*sync.RWMutex

	// rows is mapping from row key to column group key to column map
	mtx         *sync.RWMutex
	ksName      string
	tableName   string
	rows        map[rowKey]*btree.BTree
	entity      interface{}
	fieldSource map[string]interface{}
	fields      []string
	keys        Keys
	options     Options
}

type rowKey string
type superColumn struct {
	Key     key
	Columns map[string]interface{}
}

func (c *superColumn) Less(item btree.Item) bool {
	other, ok := item.(*superColumn)
	if !ok {
		return false
	}

	return c.Key.Less(other.Key)
}

type gocqlTypeInfo struct {
	proto byte
	typ   gocql.Type
}

func (t gocqlTypeInfo) New() interface{} {
	return &gocqlTypeInfo{t.proto, t.typ}
}

func (t gocqlTypeInfo) Type() gocql.Type {
	return t.typ
}

func (t gocqlTypeInfo) Version() byte {
	return t.proto
}

func (t gocqlTypeInfo) Custom() string {
	return ""
}

type keyPart struct {
	Key   string
	Value interface{}
}

func (k *keyPart) Bytes() []byte {
	typeInfo := &gocqlTypeInfo{
		proto: 0x03,
		typ:   cassaType(k.Value),
	}
	marshalled, err := gocql.Marshal(typeInfo, k.Value)
	if err != nil {
		panic(err)
	}
	return marshalled
}

type key []keyPart

func (k key) Less(other key) bool {
	for i := 0; i < len(k) && i < len(other); i++ {
		cmp := bytes.Compare(k[i].Bytes(), other[i].Bytes())
		if cmp == 0 {
			continue
		}
		return cmp < 0
	}

	return false
}

func (k key) RowKey() rowKey {
	buf := bytes.Buffer{}

	for _, part := range k {
		buf.Write(part.Bytes())
	}

	return rowKey(buf.String())
}

func (k key) ToSuperColumn() *superColumn {
	return &superColumn{Key: k}
}

func (k key) Append(column string, value interface{}) key {
	newKey := make([]keyPart, len(k)+1)
	copy(newKey, k)
	newKey[len(k)] = keyPart{column, value}
	return newKey
}

func (t *MockTable) partitionKeyFromColumnValues(values map[string]interface{}, keyNames []string) (key, error) {
	var key key

	// For a single partition key of type string, check that it is not
	// empty, this is same as this error from a real C* cluster-
	// InvalidRequest: Error from server: code=2200 [Invalid query]
	// message="Key may not be empty"
	if len(keyNames) == 1 {
		value, ok := values[keyNames[0]]
		stringVal, isString := value.(string)
		if !ok || (isString && stringVal == "") {
			return nil, fmt.Errorf("Missing mandatory PRIMARY KEY part %s", keyNames[0])
		}
		key = key.Append(keyNames[0], value)
		return key, nil
	}

	// Cassandra _does_ allow you to have a composite partition key in which
	// all the components can be empty
	for _, keyName := range keyNames {
		value, ok := values[keyName]
		if !ok {
			return nil, fmt.Errorf("Missing mandatory PRIMARY KEY part %s", keyName)
		}
		key = key.Append(keyName, value)
	}

	return key, nil
}

func (t *MockTable) clusteringKeyFromColumnValues(values map[string]interface{}, keyNames []string) (key, error) {
	var key key

	for _, keyName := range keyNames {
		value, ok := values[keyName]
		if !ok {
			return nil, fmt.Errorf("Missing mandatory PRIMARY KEY part %s", keyName)
		}
		key = key.Append(keyName, value)
	}

	return key, nil
}

func (t *MockTable) Name() string {
	if len(t.options.TableName) > 0 {
		return t.options.TableName
	}
	return t.tableName
}

func (t *MockTable) getOrCreateRow(rowKey key) *btree.BTree {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	row := t.rows[rowKey.RowKey()]
	if row == nil {
		row = btree.New(2)
		t.rows[rowKey.RowKey()] = row
	}
	return row
}

func (t *MockTable) getOrCreateColumnGroup(rowKey, superColumnKey key) map[string]interface{} {
	row := t.getOrCreateRow(rowKey)
	scol := superColumnKey.ToSuperColumn()

	if row.Has(scol) {
		return row.Get(scol).(*superColumn).Columns
	}
	row.ReplaceOrInsert(scol)
	scol.Columns = map[string]interface{}{}

	return scol.Columns
}

func (t *MockTable) SetWithOptions(i interface{}, options Options) Op {
	return newOp(func(m mockOp) error {
		t.Lock()
		defer t.Unlock()

		columns, ok := toMap(i)
		if !ok {
			return errors.New("Can't create: value not understood")
		}

		rowKey, err := t.partitionKeyFromColumnValues(columns, t.keys.PartitionKeys)
		if err != nil {
			return err
		}

		superColumnKey, err := t.clusteringKeyFromColumnValues(columns, t.keys.ClusteringColumns)
		if err != nil {
			return err
		}

		superColumn := t.getOrCreateColumnGroup(rowKey, superColumnKey)

		if err := assignRecords(columns, superColumn); err != nil {
			return err
		}
		return nil
	})
}

func (t *MockTable) Set(i interface{}) Op {
	return t.SetWithOptions(i, t.options)
}

func (t *MockTable) Where(relations ...Relation) Filter {
	return &MockFilter{
		table:     t,
		relations: relations,
	}
}

func (t *MockTable) Create() error {
	return nil
}

func (t *MockTable) CreateStatement() (Statement, error) {
	return noOpStatement{}, nil
}

func (t *MockTable) CreateIfNotExist() error {
	return nil
}

func (t *MockTable) CreateIfNotExistStatement() (Statement, error) {
	return noOpStatement{}, nil
}

func (t *MockTable) Recreate() error {
	return nil
}

func (t *MockTable) WithOptions(o Options) Table {
	return &MockTable{
		RWMutex:     t.RWMutex,
		ksName:      t.ksName,
		tableName:   t.tableName,
		rows:        t.rows,
		entity:      t.entity,
		keys:        t.keys,
		fieldSource: t.fieldSource,
		fields:      t.fields,
		options:     t.options.Merge(o),
		mtx:         t.mtx,
	}
}

// MockFilter implements the Filter interface and works with MockTable.
type MockFilter struct {
	table     *MockTable
	relations []Relation
}

func (f *MockFilter) Table() Table {
	return f.table
}

func (f *MockFilter) Relations() []Relation {
	return f.relations
}

func (f *MockFilter) rowMatch(row map[string]interface{}) bool {
	for _, relation := range f.relations {
		value := row[relation.Field()]
		if !relation.accept(value) {
			return false
		}
	}
	return true
}

func (f *MockFilter) fieldRelationMap() map[string]Relation {
	result := map[string]Relation{}

	for _, relation := range f.relations {
		result[relation.Field()] = relation
	}

	return result
}

func (f *MockFilter) fieldsFromRelations(fields []string) ([]key, error) {
	fieldRelationMap := f.fieldRelationMap()
	var rowKey key
	var result []key

	if len(fields) == 0 {
		return []key{key{}}, nil
	}

	for i, keyName := range fields {
		lastKey := i == len(fields)-1
		relation, ok := fieldRelationMap[keyName]

		if !ok {
			return nil, fmt.Errorf("Missing mandatory PRIMARY KEY part `%s`", keyName)
		}

		if relation.Comparator() != CmpEquality && !(lastKey && relation.Comparator() == CmpIn) {
			return nil, fmt.Errorf("Invalid use of PK `%s`", keyName)
		}

		if !lastKey {
			rowKey = rowKey.Append(keyName, relation.Terms()[0])
		} else {
			for _, term := range relation.Terms() {
				result = append(result, rowKey.Append(relation.Field(), term))
			}
		}
	}

	return result, nil
}

func (f *MockFilter) UpdateWithOptions(m map[string]interface{}, options Options) Op {
	return newOp(func(mock mockOp) error {
		f.table.Lock()
		defer f.table.Unlock()

		rowKeys, err := f.fieldsFromRelations(f.table.keys.PartitionKeys)
		if err != nil {
			return err
		}

		for _, rowKey := range rowKeys {
			superColumnKeys, err := f.fieldsFromRelations(f.table.keys.ClusteringColumns)
			if err != nil {
				return err
			}

			for _, superColumnKey := range superColumnKeys {
				superColumn := f.table.getOrCreateColumnGroup(rowKey, superColumnKey)

				for _, key := range []key{rowKey, superColumnKey} {
					for _, keyPart := range key {
						superColumn[keyPart.Key] = keyPart.Value
					}
				}

				if err := assignRecords(m, superColumn); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (f *MockFilter) Update(m map[string]interface{}) Op {
	return f.UpdateWithOptions(m, Options{})
}

func (f *MockFilter) Delete() Op {
	return newOp(func(m mockOp) error {
		f.table.Lock()
		defer f.table.Unlock()

		rowKeys, err := f.fieldsFromRelations(f.table.keys.PartitionKeys)
		if err != nil {
			return err
		}

		f.table.mtx.Lock()
		defer f.table.mtx.Unlock()
		for _, rowKey := range rowKeys {
			row := f.table.rows[rowKey.RowKey()]
			if row == nil {
				return nil
			}

			row.Ascend(func(item btree.Item) bool {
				columns := item.(*superColumn).Columns
				if f.rowMatch(columns) {
					row.Delete(item)
				}

				return true
			})
		}

		return nil
	})
}

func (q *MockFilter) Read(out interface{}) Op {
	return newOp(func(m mockOp) error {
		q.table.Lock()
		defer q.table.Unlock()

		var (
			result []map[string]interface{}
			err    error
		)

		switch {
		case len(q.Relations()) == 0:
			result = q.readAllRows()
		default:
			result, err = q.readSomeRows()
		}
		if err != nil {
			return err
		}

		opt := q.table.options.Merge(m.options)
		if opt.Limit > 0 && opt.Limit < len(result) {
			result = result[:opt.Limit]
		}

		fieldNames := opt.Select
		if len(opt.Select) == 0 {
			fieldNames = q.table.fields
		}

		stmt := SelectStatement{keyspace: q.table.ksName, table: q.table.Name(), fields: fieldNames}
		iter := newMockIterator(result, stmt.fields)
		_, err = NewScanner(stmt, out).ScanIter(iter)
		return err
	})
}

func (q *MockFilter) readSomeRows() ([]map[string]interface{}, error) {
	q.table.mtx.RLock()
	defer q.table.mtx.RUnlock()

	rowKeys, err := q.fieldsFromRelations(q.table.keys.PartitionKeys)
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for _, rowKey := range rowKeys {
		row := q.table.rows[rowKey.RowKey()]
		if row == nil {
			continue
		}

		row.Ascend(func(item btree.Item) bool {
			columns := item.(*superColumn).Columns
			if q.rowMatch(columns) {
				result = append(result, columns)
			}

			return true
		})
	}

	return result, nil
}

func (q *MockFilter) readAllRows() []map[string]interface{} {
	q.table.mtx.RLock()
	defer q.table.mtx.RUnlock()
	var result []map[string]interface{}
	for _, row := range q.table.rows {
		row.Ascend(func(item btree.Item) bool {
			columns := item.(*superColumn).Columns
			if q.rowMatch(columns) {
				result = append(result, columns)
			}

			return true
		})
	}
	return result
}

func (q *MockFilter) ReadOne(out interface{}) Op {
	return newOp(func(m mockOp) error {
		return q.Read(out).Run()
	})
}

// mockIterator takes in a slice of maps and implements a Scannable iterator
// which goes row by row within the slice.
type mockIterator struct {
	results      []map[string]interface{}
	fields       []string
	currRowIndex int
	closed       bool
	err          error
}

func newMockIterator(results []map[string]interface{}, fields []string) *mockIterator {
	return &mockIterator{
		results:      results,
		fields:       fields,
		currRowIndex: -1,
		closed:       false,
	}
}

// Next checks to see if there a result to be read
func (iter *mockIterator) Next() bool {
	if iter.closed {
		return false
	}

	// Check if reading the next row will get us out of bounds
	if iter.currRowIndex+1 >= len(iter.results) {
		return false
	}

	iter.currRowIndex++
	return true
}

// Scan mocks a Scanner such as the one you get in gocql.Iter to assign results
func (iter *mockIterator) Scan(dest ...interface{}) error {
	if iter.closed {
		// We don't explictly assign this error to iter.err so we don't lose the
		// original error in the iterator
		return fmt.Errorf("called iterator after resources released")
	}

	if iter.currRowIndex < 0 {
		return fmt.Errorf("called Scan without calling Next")
	}

	if len(dest) != len(iter.fields) {
		iter.err = fmt.Errorf("got %d pointers for unmarshalling %d fields", len(dest), len(iter.fields))
		return iter.err
	}

	result := iter.results[iter.currRowIndex]
	for i, fieldName := range iter.fields {
		if reflect.TypeOf(dest[i]).Kind() != reflect.Ptr {
			iter.err = fmt.Errorf("expected pointer but got %T", dest[i])
			return iter.err
		}

		value, ok := result[fieldName]
		if !ok {
			// See if any fields in result are equal (case insensitive)
			for k, v := range result {
				if strings.EqualFold(k, fieldName) {
					value = v
					ok = true
					break
				}
			}

			if !ok {
				// We could panic here but ultimately this will be the zero value of
				// the resulting pointer and is a valid use case so soldier on
				continue
			}
		}

		// If it's a field to ignore, then ignore it ;)
		rv := reflect.ValueOf(dest[i])
		if rv.Elem().Type() == reflect.TypeOf((*IgnoreFieldType)(nil)).Elem() {
			continue
		}

		sv := reflect.ValueOf(value)
		if !sv.IsValid() { //  Ensure we're not working with the zero value
			continue
		}

		// Maps are stored in the mock as map[<KeyType>]interface{}. The receiving value
		// may be of a different map type so we need to accommodate for this.
		if sv.Kind() == reflect.Map && sv.Type().Elem() != rv.Elem().Type().Elem() {
			targetMap := reflect.MakeMap(rv.Elem().Type())
			for _, key := range sv.MapKeys() {
				// Need to do a type assertion here to set the underlying rv map value type
				switch v := sv.MapIndex(key).Interface().(type) {
				case int, int8, int16, int64:
					targetMap.SetMapIndex(key, reflect.ValueOf(v))
				case float32, float64, bool:
					targetMap.SetMapIndex(key, reflect.ValueOf(v))
				case byte, []byte, string, []string:
					targetMap.SetMapIndex(key, reflect.ValueOf(v))
				case interface{}, gocql.Unmarshaler:
					targetMap.SetMapIndex(key, reflect.ValueOf(v))
				default:
					iter.err = fmt.Errorf("mock doesn't support map value type %T", v)
					return iter.err
				}
			}
			sv = targetMap
		}

		// We need to handle the case where we're given a pointer to a type which is
		// not the exact type of the value but we can convert over by casting
		if sv.Type() != rv.Elem().Type() {
			if !sv.Type().ConvertibleTo(rv.Elem().Type()) {
				iter.err = fmt.Errorf("could not unmarshal %T into %v", value, rv.Elem().Type())
				return iter.err
			}
			sv = sv.Convert(rv.Elem().Type())
		}

		rv.Elem().Set(sv)
	}

	return nil
}

// Err returns the active error in the iterator. Once called, the 'resources'
// should be released and thus this iterator is closed
func (iter *mockIterator) Err() error {
	iter.closed = true
	return iter.err
}

// Reset resets the result list to the beginning of the slice. This should
// only really be needed for tests
func (iter *mockIterator) Reset() {
	iter.closed = false
	iter.err = nil
	iter.currRowIndex = -1
}

func assignRecords(m map[string]interface{}, record map[string]interface{}) error {
	for k, v := range m {
		switch v := v.(type) {
		case Modifier:
			switch v.op {
			case ModifierMapSetField:
				// Go interfaces are internally represented as a type and a value. The record[k] interface{} value could look like one of these:
				// [type, value]
				// [type, nil  ]
				// [nil,  nil  ]
				var targetMap reflect.Value
				if record[k] != nil {
					// narrowed it down to:
					// [type, value]
					// [type, nil  ]
					rv := reflect.ValueOf(record[k])

					if rv.Type().Kind() != reflect.Map {
						return fmt.Errorf("Can't use MapSetField modifier on field that isn't a map: %T", record[k])
					}

					if !rv.IsNil() {
						// [type, value]
						targetMap = rv
					}
				}

				// This modifier's args is a []interface{} with a key at index 0 and a value at index 1
				if len(v.args) != 2 {
					return fmt.Errorf("Argument for MapSetField is not a slice of 2 elements")
				}

				key := reflect.ValueOf(v.args[0])
				value := reflect.ValueOf(v.args[1])

				// If we couldn't initialize the map from the content of record[k], we create it from the values of v.args
				if targetMap.Kind() != reflect.Map {
					targetMapType := reflect.MapOf(key.Type(), value.Type())
					targetMap = reflect.MakeMap(targetMapType)
				}

				targetMap.SetMapIndex(key, value)

				record[k] = targetMap.Interface()
			case ModifierMapSetFields:
				// Go interfaces are internally represented as a type and a value. The record[k] interface{} value could look like one of these:
				// [type, value]
				// [type, nil  ]
				// [nil,  nil  ]
				var targetMap reflect.Value
				if record[k] != nil {
					// narrowed it down to:
					// [type, value]
					// [type, nil  ]
					rv := reflect.ValueOf(record[k])

					if rv.Type().Kind() != reflect.Map {
						return fmt.Errorf("Can't use MapSetFields modifier on field that isn't a map: %T", record[k])
					}

					if rv.IsNil() {
						// [type, nil  ]
						targetMap = reflect.MakeMap(rv.Type())
					} else {
						// [type, value]
						targetMap = rv
					}
				} else {
					// [nil,  nil  ]
					// We don't know the type, so we guess. Note that this guess is
					// likely wrong but to fix that we need a much larger refactor.
					targetMap = reflect.ValueOf(map[string]interface{}{})
				}

				ma, ok := v.args[0].(map[string]interface{})
				if !ok {
					return fmt.Errorf("Argument for MapSetFields is not a map")
				}
				for k, v := range ma {
					targetMap.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(v))
				}
				record[k] = targetMap.Interface()
			case ModifierCounterIncrement:
				oldV, _ := record[k].(int64)
				delta := int64(v.args[0].(int))

				record[k] = oldV + delta
			default:
				return fmt.Errorf("Modifer %v not supported by mock keyspace", v.op)
			}
		default:
			record[k] = v
		}
	}

	return nil
}

type mockContextKey string

var errorInjectorContextKey mockContextKey = "error_injector_context_key"

type ErrorInjector interface {
	shouldReturnErr(op Op, opIdx int, opCount int) error
}

func getErrorInjector(ctx context.Context) ErrorInjector {
	if ctx != nil {
		if strategy := extractErrorInjectorFromContext(ctx); strategy != nil {
			return strategy
		}
	}
	return &neverFail{}
}

func extractErrorInjectorFromContext(ctx context.Context) ErrorInjector {
	if v := ctx.Value(errorInjectorContextKey); v != nil {
		if strategy, ok := v.(ErrorInjector); ok {
			return strategy
		}
	}
	return nil
}

// ErrorInjectorContext returns a context which when passed to
// mockMultiOp.RunWithContext(...), will inject an error before one of the
// operations to simulate a partial failure in the query execution.
// The ErrorInjector determines when in the sequence the error is injected
func ErrorInjectorContext(parent context.Context, strategy ErrorInjector) context.Context {
	return context.WithValue(parent, errorInjectorContextKey, strategy)
}

type neverFail struct{}

func (n *neverFail) shouldReturnErr(Op, int, int) error { return nil }

// FailOnNthOperation returns an ErrorInjector which injects the provided err on
// the nth operation of a mockMultiOp. n is 0 indexed so an n value of 0 will
// fail on the first operation. If there are fewer than n-1 operations in the
// mockMultiOp, no error will be injected
func FailOnNthOperation(n int, err error) ErrorInjector {
	return &failOnNthOperation{n: n, err: err}
}

type failOnNthOperation struct {
	n   int
	err error
}

func (f *failOnNthOperation) shouldReturnErr(op Op, opIdx, opCount int) error {
	if opIdx == f.n {
		return f.err
	}
	return nil
}

// FailOnEachOperation returns an ErrorInjector which fails on each operation of
// a mockMultiOp in turn
func FailOnEachOperation(err error) *FailOnEachOperationErrorInjector {
	return &FailOnEachOperationErrorInjector{
		err:                    err,
		finalOpSucceeded:       false,
		lastErrorInjectedAtIdx: -1,
	}
}

// FailOnEachOperationErrorInjector is a stateful ErrorInjector which fails on
// each each operation of a mockMultiOp. The caller should repeatedly call the
// function under test until ShouldContinue returns false.
type FailOnEachOperationErrorInjector struct {
	shouldFailOnOp         int
	lastErrorInjectedAtIdx int
	finalOpSucceeded       bool
	err                    error
}

// LastErrorInjectedAtIdx indicates the index of the operation the error
// injector last injected an error at. Returns -1 if no error was injected
func (f *FailOnEachOperationErrorInjector) LastErrorInjectedAtIdx() int {
	return f.lastErrorInjectedAtIdx
}

// ShouldContinue indicates whether there are more operations in the mockMultiOp
// in which to inject errors. Returns false when the final operation in the
// mockMultiOp succeeded
func (f *FailOnEachOperationErrorInjector) ShouldContinue() bool {
	if f.finalOpSucceeded {
		return false
	}
	return true
}

func (f *FailOnEachOperationErrorInjector) shouldReturnErr(op Op, opIdx, opCount int) error {
	if opIdx == f.shouldFailOnOp {
		f.shouldFailOnOp++
		f.lastErrorInjectedAtIdx = opIdx
		return f.err
	}
	if opIdx == opCount-1 {
		f.lastErrorInjectedAtIdx = -1
		f.finalOpSucceeded = true
	}
	return nil
}

func ExampleFailOnEachOperation() {
	ks := NewMockKeySpace()
	type Thing struct {
		ID    string
		Field string
	}

	table := ks.MapTable(
		"table_name",
		"ID",
		Thing{},
	)

	things := []Thing{
		{ID: "1", Field: "one"},
		{ID: "2", Field: "two"},
		{ID: "3", Field: "three"},
		{ID: "4", Field: "four"},
		{ID: "5", Field: "five"},
	}
	op := Noop()
	for _, thing := range things {
		op = op.Add(table.Set(thing))
	}

	errToInject := fmt.Errorf("injected error")
	errorInjector := FailOnEachOperation(errToInject)
	ctx := ErrorInjectorContext(context.Background(), errorInjector)

	for errorInjector.ShouldContinue() {
		if err := op.RunWithContext(ctx); err != nil {
			fmt.Printf(
				"received error: %s on operation: %d\n",
				err,
				errorInjector.LastErrorInjectedAtIdx(),
			)
		}
	}
}
