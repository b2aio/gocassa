package gocassa

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

const (
	// These comparison types represent the comparison types supported
	// when generating a relation between a key and it's terms
	CmpEquality            = iota // direct equality (foo = bar)
	CmpIn                         // membership (foo IN (bar, bing, baz))
	CmpGreaterThan                // larger than (foo > 1)
	CmpGreaterThanOrEquals        // larger than or equal (foo >= 1)
	CmpLesserThan                 // less than (foo < 1)
	CmpLesserThanOrEquals         // less than or equal (foo <= 1)
)

type Relation struct {
	cmpType int
	key     string
	terms   []interface{}
}

func (r Relation) cql() (string, []interface{}) {
	ret := ""
	key := strings.ToLower(r.key)
	switch r.cmpType {
	case CmpEquality:
		ret = key + " = ?"
	case CmpIn:
		return key + " IN ?", r.terms
	case CmpGreaterThan:
		ret = key + " > ?"
	case CmpGreaterThanOrEquals:
		ret = key + " >= ?"
	case CmpLesserThan:
		ret = key + " < ?"
	case CmpLesserThanOrEquals:
		ret = key + " <= ?"
	}
	return ret, r.terms
}

func anyEquals(value interface{}, terms []interface{}) bool {
	primVal := convertToPrimitive(value)
	for _, term := range terms {
		if primVal == convertToPrimitive(term) {
			return true
		}
	}
	return false
}

func convertToPrimitive(i interface{}) interface{} {
	switch v := i.(type) {
	case time.Time:
		return v.UnixNano()
	case time.Duration:
		return v.Nanoseconds()
	case []byte:
		// This case works as strings in Go are simply defined as the following:
		// "A string value is a (possibly empty) sequence of bytes" (from the go lang spec)
		// and
		// "Converting a slice of bytes to a string type yields a string whose successive bytes are the elements of the slice."
		// Finally:
		// "String values are comparable and ordered, lexically byte-wise."
		// We mostly want this to allow comparisons of blob types in the primary key of a table,
		// since []byte are not `==` comparable in go, but strings are
		return string(v)
	default:
		// If the underlying type is a string, we want to represent this value
		// as a string for comparison across proxy types.
		if reflect.ValueOf(i).Kind() == reflect.String {
			return fmt.Sprintf("%v", i)
		}
		return i
	}
}

func (r Relation) accept(i interface{}) bool {
	var result bool
	var err error

	if r.cmpType == CmpEquality || r.cmpType == CmpIn {
		return anyEquals(i, r.terms)
	}

	a, b := convertToPrimitive(i), convertToPrimitive(r.terms[0])

	switch r.cmpType {
	case CmpGreaterThan:
		result, err = builtinGreaterThan(a, b)
	case CmpGreaterThanOrEquals:
		result, err = builtinGreaterThan(a, b)
		result = result || a == b
	case CmpLesserThanOrEquals:
		result, err = builtinLessThan(a, b)
		result = result || a == b
	case CmpLesserThan:
		result, err = builtinLessThan(a, b)
	}

	return err == nil && result
}

func toI(i interface{}) []interface{} {
	return []interface{}{i}
}

func Eq(key string, term interface{}) Relation {
	return Relation{
		cmpType: CmpEquality,
		key:     key,
		terms:   toI(term),
	}
}

func In(key string, terms ...interface{}) Relation {
	return Relation{
		cmpType: CmpIn,
		key:     key,
		terms:   terms,
	}
}

func GT(key string, term interface{}) Relation {
	return Relation{
		cmpType: CmpGreaterThan,
		key:     key,
		terms:   toI(term),
	}
}

func GTE(key string, term interface{}) Relation {
	return Relation{
		cmpType: CmpGreaterThanOrEquals,
		key:     key,
		terms:   toI(term),
	}
}

func LT(key string, term interface{}) Relation {
	return Relation{
		cmpType: CmpLesserThan,
		key:     key,
		terms:   toI(term),
	}
}

func LTE(key string, term interface{}) Relation {
	return Relation{
		cmpType: CmpLesserThanOrEquals,
		key:     key,
		terms:   toI(term),
	}
}
