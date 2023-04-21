package gocassa

type multimapMkT struct {
	table               Table
	partitionKeyFields  []string
	clusteringKeyFields []string
}

func (mm *multimapMkT) Table() Table                        { return mm.table }
func (mm *multimapMkT) Create() error                       { return mm.Table().Create() }
func (mm *multimapMkT) CreateIfNotExist() error             { return mm.Table().CreateIfNotExist() }
func (mm *multimapMkT) Name() string                        { return mm.Table().Name() }
func (mm *multimapMkT) Recreate() error                     { return mm.Table().Recreate() }
func (mm *multimapMkT) CreateStatement() (Statement, error) { return mm.Table().CreateStatement() }
func (mm *multimapMkT) CreateIfNotExistStatement() (Statement, error) {
	return mm.Table().CreateIfNotExistStatement()
}

func (mm *multimapMkT) Update(partitionKeys, clusteringKeys map[string]interface{}, m map[string]interface{}) Op {
	return mm.Table().
		Where(mm.ListOfEqualRelations(partitionKeys, clusteringKeys)...).
		Update(m)
}

func (mm *multimapMkT) Set(entity interface{}) Op {
	return mm.Table().
		Set(entity)
}

func (mm *multimapMkT) Delete(partitionKeys, clusteringKeys map[string]interface{}) Op {
	return mm.Table().
		Where(mm.ListOfEqualRelations(partitionKeys, clusteringKeys)...).
		Delete()
}

func (mm *multimapMkT) DeleteAll(partitionKeys map[string]interface{}) Op {
	return mm.Table().
		Where(mm.ListOfEqualRelations(partitionKeys, nil)...).
		Delete()
}

func (mm *multimapMkT) Read(partitionKeys, clusteringKeys map[string]interface{}, pointer interface{}) Op {
	return mm.Table().
		Where(mm.ListOfEqualRelations(partitionKeys, clusteringKeys)...).
		ReadOne(pointer)
}

func (mm *multimapMkT) MultiRead(partitionKeys, clusteringKeys map[string]interface{}, pointerToASlice interface{}) Op {
	return mm.Table().
		Where(mm.ListOfEqualRelations(partitionKeys, clusteringKeys)...).
		Read(pointerToASlice)
}

func (mm *multimapMkT) List(partitionKeys, fromClusteringKeys map[string]interface{}, limit int, pointerToASlice interface{}) Op {
	rels := mm.ListOfEqualRelations(partitionKeys, nil)
	if fromClusteringKeys != nil {
		for _, field := range mm.clusteringKeyFields {
			if value := fromClusteringKeys[field]; value != "" {
				rels = append(rels, GTE(field, value))
			}
		}
	}
	return mm.
		WithOptions(Options{
			Limit: limit,
		}).
		Table().
		Where(rels...).
		Read(pointerToASlice)
}

func (mm *multimapMkT) WithOptions(o Options) MultimapMkTable {
	return &multimapMkT{
		table:               mm.Table().WithOptions(o),
		partitionKeyFields:  mm.partitionKeyFields,
		clusteringKeyFields: mm.clusteringKeyFields,
	}
}

func (mm *multimapMkT) ListOfEqualRelations(partitionKeys, clusteringKeys map[string]interface{}) []Relation {
	relations := make([]Relation, 0)

	for _, field := range mm.partitionKeyFields {
		if value := partitionKeys[field]; value != nil && value != "" {
			relation := Eq(field, value)
			relations = append(relations, relation)
		}
	}

	for _, field := range mm.clusteringKeyFields {
		if value := clusteringKeys[field]; value != nil && value != "" {
			relation := Eq(field, value)
			relations = append(relations, relation)
		}
	}

	return relations
}
