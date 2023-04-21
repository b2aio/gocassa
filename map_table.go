package gocassa

type mapT struct {
	table             Table
	partitionKeyField string
}

func (m *mapT) Table() Table                        { return m.table }
func (m *mapT) Create() error                       { return m.Table().Create() }
func (m *mapT) CreateIfNotExist() error             { return m.Table().CreateIfNotExist() }
func (m *mapT) Name() string                        { return m.Table().Name() }
func (m *mapT) Recreate() error                     { return m.Table().Recreate() }
func (m *mapT) CreateStatement() (Statement, error) { return m.Table().CreateStatement() }
func (m *mapT) CreateIfNotExistStatement() (Statement, error) {
	return m.Table().CreateIfNotExistStatement()
}

func (m *mapT) Update(partitionKey interface{}, fieldMap map[string]interface{}) Op {
	return m.Table().
		Where(Eq(m.partitionKeyField, partitionKey)).
		Update(fieldMap)
}

func (m *mapT) Set(entity interface{}) Op {
	return m.Table().
		Set(entity)
}

func (m *mapT) Delete(partitionKey interface{}) Op {
	return m.Table().
		Where(Eq(m.partitionKeyField, partitionKey)).
		Delete()
}

func (m *mapT) Read(partitionKey, pointer interface{}) Op {
	return m.Table().
		Where(Eq(m.partitionKeyField, partitionKey)).
		ReadOne(pointer)
}

func (m *mapT) MultiRead(partitionKey []interface{}, pointerToASlice interface{}) Op {
	return m.Table().
		Where(In(m.partitionKeyField, partitionKey...)).
		Read(pointerToASlice)
}

func (m *mapT) WithOptions(o Options) MapTable {
	return &mapT{
		table:             m.Table().WithOptions(o),
		partitionKeyField: m.partitionKeyField,
	}
}
