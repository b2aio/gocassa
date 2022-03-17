package gocassa

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	ks KeySpace
	
	prodEntityMapIDTable MapTable
	mockEntityMapIDTable *MockMapTable 
)

type entity struct {
	ID string 
	Field1 string
}

func InitMocks() {
	ks = NewMockKeySpace()

	mockEntityMapIDTable = NewMockMapTable(ks.MapTable(
		"entity",
		"ID",
		entity{},
	))
	prodEntityMapIDTable = mockEntityMapIDTable
}

func SetEntity(ctx context.Context, e *entity) error {
	return prodEntityMapIDTable.Set(e).RunWithContext(ctx)
}

func TestSetEntity(t *testing.T) {
	InitMocks()
	ctxBg := context.Background()

	e := &entity{ID: "abc", Field1: "def"}

	mockEntityMapIDTable.OnOp(t, "Set", mock.Anything).Return(fmt.Errorf("error"))

	require.Error(t, SetEntity(ctxBg, e))
	
}
