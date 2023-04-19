package gocassa

type multimapT struct {
	t                  Table
	partitionKeyField  string
	clusteringKeyField string
}

func (mm *multimapT) Table() Table                        { return mm.t }
func (mm *multimapT) Create() error                       { return mm.Table().Create() }
func (mm *multimapT) CreateIfNotExist() error             { return mm.Table().CreateIfNotExist() }
func (mm *multimapT) Name() string                        { return mm.Table().Name() }
func (mm *multimapT) Recreate() error                     { return mm.Table().Recreate() }
func (mm *multimapT) CreateStatement() (Statement, error) { return mm.Table().CreateStatement() }
func (mm *multimapT) CreateIfNotExistStatement() (Statement, error) {
	return mm.Table().CreateIfNotExistStatement()
}

func (mm *multimapT) Update(field, id interface{}, m map[string]interface{}) Op {
	return mm.Table().
		Where(Eq(mm.partitionKeyField, field),
			Eq(mm.clusteringKeyField, id)).
		Update(m)
}

func (mm *multimapT) Set(v interface{}) Op {
	return mm.Table().
		Set(v)
}

func (mm *multimapT) Delete(partitionKey, clusteringKey interface{}) Op {
	return mm.Table().
		Where(Eq(mm.partitionKeyField, partitionKey), Eq(mm.clusteringKeyField, clusteringKey)).
		Delete()
}

func (mm *multimapT) DeleteAll(partitionKey interface{}) Op {
	return mm.Table().
		Where(Eq(mm.partitionKeyField, partitionKey)).
		Delete()
}

func (mm *multimapT) Read(partitionKey, clusteringKey, pointer interface{}) Op {
	return mm.Table().
		Where(Eq(mm.partitionKeyField, partitionKey),
			Eq(mm.clusteringKeyField, clusteringKey)).
		ReadOne(pointer)
}

func (mm *multimapT) List(partitionKey, fromClusteringKey interface{}, limit int, pointerToASlice interface{}) Op {
	rels := []Relation{Eq(mm.partitionKeyField, partitionKey)}
	if fromClusteringKey != nil {
		rels = append(rels, GTE(mm.clusteringKeyField, fromClusteringKey))
	}
	return mm.Table().
		WithOptions(Options{
			Limit: limit,
		}).
		Where(rels...).
		Read(pointerToASlice)
}

func (mm *multimapT) WithOptions(o Options) MultimapTable {
	return &multimapT{
		t:                  mm.Table().WithOptions(o),
		partitionKeyField:  mm.partitionKeyField,
		clusteringKeyField: mm.clusteringKeyField,
	}
}
