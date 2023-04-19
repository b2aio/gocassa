package gocassa

import (
	"reflect"
	"strings"

	r "github.com/monzo/gocassa/reflect"
)

type table struct {
	keySpace *k
	info     *tableInfo
	options  Options
}

// Contains mostly analyzed information about the entity
type tableInfo struct {
	keyspace, name string
	marshalSource  interface{}
	fieldSource    map[string]interface{}
	keys           Keys
	fieldNames     map[string]struct{} // This is here only to check containment
	fields         []string
	fieldValues    []interface{}
}

func newTableInfo(keyspace, name string, keys Keys, entity interface{}, fieldSource map[string]interface{}) *tableInfo {
	cinf := &tableInfo{
		keyspace:      keyspace,
		name:          name,
		marshalSource: entity,
		keys:          keys,
		fieldSource:   fieldSource,
	}
	fields := make([]string, 0, len(fieldSource))
	values := make([]interface{}, 0, len(fieldSource))
	for _, k := range sortedKeys(fieldSource) {
		fields = append(fields, k)
		values = append(values, fieldSource[k])
	}
	cinf.fieldNames = map[string]struct{}{}
	for _, v := range fields {
		cinf.fieldNames[v] = struct{}{}
	}
	cinf.fields = fields
	cinf.fieldValues = values
	return cinf
}

func toMap(i interface{}) (m map[string]interface{}, ok bool) {
	switch v := i.(type) {
	case map[string]interface{}:
		m, ok = v, true
	default:
		m, ok = r.StructToMap(i)
	}

	return
}

func (t table) Where(rs ...Relation) Filter {
	return filter{
		table:     t,
		relations: rs,
	}
}

func (t table) generateFieldList(sel []string) []string {
	xs := make([]string, len(t.info.fields))
	if len(sel) > 0 {
		xs = sel
	} else {
		for i, v := range t.info.fields {
			xs[i] = strings.ToLower(v)
		}
	}
	return xs
}

func relations(keys Keys, m map[string]interface{}) []Relation {
	ret := []Relation{}
	for _, v := range append(keys.PartitionKeys, keys.ClusteringColumns...) {
		ret = append(ret, Eq(v, m[v]))
	}
	return ret
}

func removeFields(m map[string]interface{}, s []string) map[string]interface{} {
	keys := map[string]bool{}
	for _, v := range s {
		v = strings.ToLower(v)
		keys[v] = true
	}
	ret := map[string]interface{}{}
	for k, v := range m {
		k = strings.ToLower(k)
		if !keys[k] {
			ret[k] = v
		}
	}
	return ret
}

func transformFields(m map[string]interface{}) {
	for k, v := range m {
		switch t := v.(type) {
		case Counter:
			m[k] = CounterIncrement(int(t))
		}
	}
}

// allFieldValuesAreNullable checks if all the fields passed in contain
// items that will eventually be marked in Cassandra as null (so we can
// better predict if we should do an INSERT or an UPSERT). This is to
// protect against https://issues.apache.org/jira/browse/CASSANDRA-11805
func allFieldValuesAreNullable(fields map[string]interface{}) bool {
	for _, value := range fields {
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Array, reflect.Slice, reflect.Map:
			if rv.Len() > 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func (t table) Set(i interface{}) Op {
	m, ok := toMap(i)
	if !ok {
		panic("SetWithOptions: Incompatible type")
	}
	ks := append(t.info.keys.PartitionKeys, t.info.keys.ClusteringColumns...)
	updFields := removeFields(m, ks)
	if len(updFields) == 0 || allFieldValuesAreNullable(updFields) {
		return newWriteOp(t.keySpace.qe, filter{table: t}, insertOpType, m)
	}
	transformFields(updFields)
	rels := relations(t.info.keys, m)
	return newWriteOp(t.keySpace.qe, filter{
		table:     t,
		relations: rels,
	}, updateOpType, updFields)
}

func (t table) Create() error {
	if stmt, err := t.CreateStatement(); err != nil {
		return err
	} else {
		return t.keySpace.qe.Execute(stmt)
	}
}

func (t table) CreateIfNotExist() error {
	if stmt, err := t.CreateIfNotExistStatement(); err != nil {
		return err
	} else {
		return t.keySpace.qe.Execute(stmt)
	}
}

func (t table) Recreate() error {
	if ex, err := t.keySpace.Exists(t.Name()); ex && err == nil {
		if err := t.keySpace.DropTable(t.Name()); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return t.Create()
}

func (t table) CreateStatement() (Statement, error) {
	return createTable(t.keySpace.name,
		t.Name(),
		t.info.keys.PartitionKeys,
		t.info.keys.ClusteringColumns,
		t.info.fields,
		t.info.fieldValues,
		t.options.ClusteringOrder,
		t.info.keys.Compound,
		t.options.CompactStorage,
		t.options.Compressor,
	)
}

func (t table) CreateIfNotExistStatement() (Statement, error) {
	return createTableIfNotExist(t.keySpace.name,
		t.Name(),
		t.info.keys.PartitionKeys,
		t.info.keys.ClusteringColumns,
		t.info.fields,
		t.info.fieldValues,
		t.options.ClusteringOrder,
		t.info.keys.Compound,
		t.options.CompactStorage,
		t.options.Compressor,
	)
}

func (t table) Name() string {
	if len(t.options.TableName) > 0 {
		return t.options.TableName
	}
	return t.info.name
}

func (t table) WithOptions(o Options) Table {
	return table{
		keySpace: t.keySpace,
		info:     t.info,
		options:  t.options.Merge(o),
	}
}
