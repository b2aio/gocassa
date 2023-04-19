package gocassa

type filter struct {
	table     table
	relations []Relation
}

func (f filter) Table() Table {
	return f.table
}

func (f filter) Relations() []Relation {
	return f.relations
}

func (f filter) Update(m map[string]interface{}) Op {
	return newWriteOp(f.table.keySpace.qe, f, updateOpType, m)
}

func (f filter) Delete() Op {
	return newWriteOp(f.table.keySpace.qe, f, deleteOpType, nil)
}

//
// Reads
//

func (f filter) Read(pointerToASlice interface{}) Op {
	return &singleOp{
		qe:     f.table.keySpace.qe,
		filter: f,
		opType: readOpType,
		result: pointerToASlice}
}

func (f filter) ReadOne(pointer interface{}) Op {
	return &singleOp{
		qe:     f.table.keySpace.qe,
		filter: f,
		opType: singleReadOpType,
		result: pointer}
}
