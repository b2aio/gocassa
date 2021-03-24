package gocassa

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/gocql/gocql"

	r "github.com/monzo/gocassa/reflect"
)

// scanner implements the Scanner interface which takes in a Scannable
// iterator and is responsible for unmarshalling into the struct or slice
// of structs provided.
type scanner struct {
	stmt SelectStatement

	result      interface{}
	rowsScanned int
}

func NewScanner(stmt SelectStatement, result interface{}) Scanner {
	return &scanner{
		stmt:        stmt,
		result:      result,
		rowsScanned: 0,
	}
}

func (s *scanner) ScanIter(iter Scannable) (int, error) {
	switch getNonPtrType(reflect.TypeOf(s.result)).Kind() {
	case reflect.Slice:
		return s.iterSlice(iter)
	case reflect.Struct:
		// We are reading a single element here, decode a single row
		return s.iterSingle(iter)
	}
	return 0, fmt.Errorf("can only decode into a struct or slice of structs, not %T", s.result)
}

func (s *scanner) Result() interface{} {
	return s.result
}

func (s *scanner) iterSlice(iter Scannable) (int, error) {
	// If we're given a pointer address to nil, we are responsible for
	// allocating it before we assign. Note that this could be a ptr to
	// a ptr (and so forth)
	err := allocateNilReference(s.result)
	if err != nil {
		return 0, err
	}

	// Extract the type of the slice
	sliceType := getNonPtrType(reflect.TypeOf(s.result))
	sliceElemType := sliceType.Elem()
	sliceElemValType := getNonPtrType(sliceType.Elem())

	// To preserve prior behaviour, if the result slice is not empty
	// then allocate a new slice and set it as the value
	sliceElem := reflect.ValueOf(s.result)
	for sliceElem.Kind() == reflect.Ptr {
		sliceElem = sliceElem.Elem()
	}
	if sliceElem.Len() != 0 {
		sliceElem.Set(reflect.Zero(sliceType))
	}

	// Extract the type of the underlying struct
	structFields, err := s.structFields(sliceElemValType)
	if err != nil {
		return 0, err
	}

	rowsScanned := 0
	for iter.Next() {
		outVal := reflect.New(sliceElemValType).Elem()
		ptrs := generatePtrs(structFields, outVal)
		err := iter.Scan(ptrs...)
		if err != nil {
			return rowsScanned, err
		}

		sliceElem.Set(reflect.Append(sliceElem, wrapPtrValue(outVal, sliceElemType)))
		rowsScanned++
	}

	s.rowsScanned += rowsScanned
	return rowsScanned, nil
}

func (s *scanner) iterSingle(iter Scannable) (int, error) {
	// If we're given a pointer address to nil, we are responsible for
	// allocating it before we assign. Note that this could be a ptr to
	// a ptr (and so forth)
	err := allocateNilReference(s.result)
	if err != nil {
		return 0, err
	}

	outPtr := reflect.ValueOf(s.result)
	outVal := outPtr.Elem()
	for outVal.Kind() == reflect.Ptr {
		outVal = outVal.Elem() // we will eventually get to the underlying value
	}

	// Extract the type of the underlying struct
	resultBaseType := getNonPtrType(reflect.TypeOf(s.result))
	structFields, err := s.structFields(resultBaseType)
	if err != nil {
		return 0, err
	}

	ptrs := generatePtrs(structFields, outVal)
	if !iter.Next() {
		err := iter.Err()
		if err == nil || err == gocql.ErrNotFound {
			return 0, RowNotFoundError{}
		}
		return 0, err
	}
	err = iter.Scan(ptrs...) // we only need to scan once
	if err != nil {
		return 0, err
	}

	s.rowsScanned++
	return 1, nil
}

// structFields matches the SelectStatement field names selected to names of
// fields within the target struct type
func (s *scanner) structFields(structType reflect.Type) ([]*r.Field, error) {
	fmPtr := reflect.New(structType).Interface()
	m, err := r.StructFieldMap(fmPtr, true)
	if err != nil {
		return nil, fmt.Errorf("could not decode struct of type %T: %v", fmPtr, err)
	}

	structFields := make([]*r.Field, len(s.stmt.fields))
	for i, fieldName := range s.stmt.fields {
		field, ok := m[strings.ToLower(fieldName)]
		if !ok { // the field doesn't have a destination
			structFields[i] = nil
		} else {
			structFields[i] = &field
		}
	}
	return structFields, nil
}

// generatePtrs takes in a list of fields and the target struct value
// and generates a list of interface pointers
//
// If a structField is nil, it means it couldn't be matched and we insert
// a ignoreFieldType pointer instead. This means you will always get back
// len(structFields) pointers initialized
func generatePtrs(structFields []*r.Field, structVal reflect.Value) []interface{} {
	ptrs := make([]interface{}, len(structFields))
	for i, field := range structFields {
		if field == nil {
			ptrs[i] = &ignoreFieldType{}
			continue
		}

		// Handle the case where the embedded struct hasn't been allocated yet
		// if it's a pointer. Because these are anonymous, if they are nil we
		// can't access them! We could be smarter here by allocating embedded
		// pointers (if they aren't allocated already) and traversing the
		// struct allocating all the way down as necessary
		if len(field.Index()) > 1 {
			elem := structVal.FieldByIndex([]int{field.Index()[0]})
			if elem.Kind() == reflect.Ptr && elem.IsNil() {
				ptrs[i] = &ignoreFieldType{}
				continue
			}
		}

		elem := structVal.FieldByIndex(field.Index())
		if !elem.CanSet() {
			ptrs[i] = &ignoreFieldType{}
			continue
		}

		switch elem.Kind() {
		case reflect.Map:
			elem.Set(reflect.MakeMap(elem.Type()))
		case reflect.Slice:
			elem.Set(reflect.MakeSlice(elem.Type(), 0, 0))
		}

		ptrs[i] = elem.Addr().Interface()
	}
	return ptrs
}

// allocateNilReference checks to see if the in is not nil itself but points to
// an object which itself is nil. Note that it only checks one depth down.
// Returns true if any allocation has happened, false if no allocation was needed
func allocateNilReference(in interface{}) error {
	val := reflect.ValueOf(in)
	if val.Kind() != reflect.Ptr {
		return nil
	}

	if val.IsNil() {
		return fmt.Errorf("pointer passed in was nil itself (not addressable)")
	}

	// Don't re-allocate if we don't need to. If the underlying element is not
	// nil then we can avoid an alloc (only checks depth = 1)
	switch val.Elem().Kind() {
	case reflect.Map, reflect.Slice:
		if !val.Elem().IsNil() {
			return nil
		}
	}

	// Here we unravel the underlying base type, it's just
	// pointer turtles all the way down
	topLevelType := reflect.TypeOf(in)
	baseType := reflect.TypeOf(in)
	for baseType.Kind() == reflect.Ptr {
		baseType = baseType.Elem()
	}

	var basePtr reflect.Value
	switch baseType.Kind() {
	case reflect.Array, reflect.Chan, reflect.Func, reflect.Interface, reflect.Ptr, reflect.UnsafePointer:
		return fmt.Errorf("type of kind %v is not supported", baseType.Kind())
	case reflect.Map:
		basePtr = reflect.MakeMap(baseType)
	case reflect.Slice:
		basePtr = reflect.MakeSlice(baseType, 0, 0)
	default:
		basePtr = reflect.New(baseType)
	}

	// Then we work our way backwards by wrapping pointers with
	// more pointers until we get the result type
	resultPtr := wrapPtrValue(basePtr, topLevelType)
	reflect.ValueOf(in).Elem().Set(resultPtr.Elem())
	return nil
}

// getNonPtrType keeps digging to find the top level non-pointer type
// of an instance (of a struct or otherwise) passed in. For example:
//  - If you pass in a int, you'll get back int
//  - If you pass in a *int, you'll get back int
//  - If you pass in a *[]*int, you'll get back []*int
//  - If you pass in a **[]*int, you'll get back []*int
func getNonPtrType(in reflect.Type) reflect.Type {
	elem := in
	for elem.Kind() == reflect.Ptr {
		elem = elem.Elem()
	}
	return elem
}

// wrapPtrValue takes in a value and keeps wrapping in pointers until it
// reaches the target type
func wrapPtrValue(ptr reflect.Value, target reflect.Type) reflect.Value {
	resultPtr := ptr
	for resultPtr.Type() != target {
		ptr := reflect.New(resultPtr.Type())
		ptr.Elem().Set(resultPtr)
		resultPtr = ptr
	}
	return resultPtr
}

// ignoreFieldType struct is for fields we want to ignore, we specify a custom
// unmarshal type which literally is a no-op and does nothing with this data.
// In the future, maybe we can be smarter of only extracting fields which we
// are able to unmarshal into our target struct and get rid of this
type ignoreFieldType struct{}

func (i *ignoreFieldType) UnmarshalCQL(_ gocql.TypeInfo, _ []byte) error {
	return nil
}
