package gocassa 

import (
	"testing"

	"github.com/stretchr/testify/mock"
)

type MockMapTable struct {
	MapTable
	mock.Mock

	mockOps []*mockOp
}

func NewMockMapTable(table MapTable) *MockMapTable {
	return &MockMapTable{MapTable: table, mockOps: []*mockOp{}}
}

func (m *MockMapTable) Set(rowStruct interface{}) (op Op) {
	// Deferred functions are executed after any result parameters are set by
	// that return statement but before the function returns to its caller. On 
	// See the following links for more info:
	// - https://stackoverflow.com/questions/53918738/return-value-in-function-when-using-defer
	// - https://go.dev/blog/defer-panic-and-recover 
    defer func() {
        if r := recover(); r != nil {
			op = m.MapTable.Set(rowStruct)
        }
    }()

	returnArgs := m.MethodCalled("Set", rowStruct)
	if returnFunc, ok := returnArgs.Get(0).(func(rowStruct interface{}) Op); ok {
		op = returnFunc(rowStruct)
	} else {
		op = returnArgs.Get(0).(Op)
	}
	return 
}

func (m *MockMapTable) Update(partitionKey interface{}, valuesToUpdate map[string]interface{}) (op Op) {
    defer func() {
        if r := recover(); r != nil {
			op = m.MapTable.Update(partitionKey, valuesToUpdate)
        }
    }()

	returnArgs := m.Called()
	if returnFunc, ok := returnArgs.Get(0).(func(partitionKey interface{}, valuesToUpdate map[string]interface{}) Op); ok {
		op = returnFunc(partitionKey, valuesToUpdate)
	} else {
		op = returnArgs.Get(0).(Op)
	}
	return op 
}

func (m *MockMapTable) Delete(partitionKey interface{}) (op Op) {
    defer func() {
        if r := recover(); r != nil {
			op = m.MapTable.Delete(partitionKey)
        }
    }()

	returnArgs := m.Called()
	if returnFunc, ok := returnArgs.Get(0).(func(partitionKey interface{}) Op); ok {
		op = returnFunc(partitionKey)
	} else {
		op = returnArgs.Get(0).(Op)
	}
	return op 
}

func (m *MockMapTable) Read(partitionKey, pointer interface{}) (op Op) {
    defer func() {
        if r := recover(); r != nil {
			op = m.MapTable.Read(partitionKey, pointer)
        }
    }()

	returnArgs := m.Called()
	if returnFunc, ok := returnArgs.Get(0).(func(partitionKey, pointer interface{}) Op); ok {
		op = returnFunc(partitionKey, pointer)
	} else {
		op = returnArgs.Get(0).(Op)
	}
	return op 
}

func (m *MockMapTable) OnOp(t *testing.T, methodName string, arguments ...interface{}) *mock.Call {
	op := &mockOp{} 
	m.mockOps = append(m.mockOps, op)
	m.On(methodName, arguments...).Return(op)
	return op.On("Run")
}

// func (m *MockMapTable) AssertExpectations(t mock.TestingT) bool {
// 	for _, op := range m.mockOps {
// 		if !op.AssertExpectations(t) {
// 			return false
// 		}
// 	}
// 	return true
// }
