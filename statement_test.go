package gocassa

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectStatement(t *testing.T) {
	fields := []string{"a", "b", "c"}
	keys := Keys{PartitionKeys: []string{"a"}}
	stmt, err := NewSelectStatement("ks1", "tbl1", fields, nil, keys)
	assert.NoError(t, err)
	assert.Equal(t, "SELECT a, b, c FROM ks1.tbl1", stmt.Query())
	assert.Equal(t, []interface{}{}, stmt.Values())

	stmt = stmt.WithLimit(10)
	assert.Equal(t, "SELECT a, b, c FROM ks1.tbl1 LIMIT ?", stmt.Query())
	assert.Equal(t, []interface{}{10}, stmt.Values())

	stmt = stmt.WithOrderBy([]ClusteringOrderColumn{
		{Column: "a", Direction: ASC},
	})
	assert.Equal(t, "SELECT a, b, c FROM ks1.tbl1 ORDER BY a ASC LIMIT ?", stmt.Query())
	assert.Equal(t, []interface{}{10}, stmt.Values())

	stmt = stmt.WithRelations([]Relation{
		Eq("foo", "bar"),
		In("baz", "bing"),
	})
	assert.Equal(t, "SELECT a, b, c FROM ks1.tbl1 WHERE foo = ? AND baz IN ? ORDER BY a ASC LIMIT ?", stmt.Query())
	assert.Equal(t, []interface{}{"bar", []interface{}{"bing"}, 10}, stmt.Values())

	stmt = stmt.WithAllowFiltering(true)
	assert.Equal(t, "SELECT a, b, c FROM ks1.tbl1 WHERE foo = ? AND baz IN ? ORDER BY a ASC LIMIT ? ALLOW FILTERING", stmt.Query())
	assert.Equal(t, []interface{}{"bar", []interface{}{"bing"}, 10}, stmt.Values())
}

func TestInsertStatement(t *testing.T) {
	fieldMap := map[string]interface{}{"a": "b"}
	keys := Keys{PartitionKeys: []string{"a"}}
	stmt, err := NewInsertStatement("ks1", "tbl1", fieldMap, keys)
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO ks1.tbl1 (a) VALUES (?)", stmt.Query())
	assert.Equal(t, []interface{}{"b"}, stmt.Values())

	fieldMap = map[string]interface{}{"a": "b", "c": "d"}
	stmt, err = NewInsertStatement("ks1", "tbl1", fieldMap, keys)
	assert.Equal(t, "INSERT INTO ks1.tbl1 (a, c) VALUES (?, ?)", stmt.Query())
	assert.Equal(t, []interface{}{"b", "d"}, stmt.Values())

	stmt = stmt.WithTTL(1 * time.Hour)
	assert.Equal(t, "INSERT INTO ks1.tbl1 (a, c) VALUES (?, ?) USING TTL ?", stmt.Query())
	assert.Equal(t, []interface{}{"b", "d", 3600}, stmt.Values())

	stmt = stmt.WithIfNotExists()
	assert.Equal(t, "INSERT INTO ks1.tbl1 (a, c) VALUES (?, ?) USING TTL ? IF NOT EXISTS", stmt.Query())
}

func TestUpdateStatement(t *testing.T) {
	fieldMap := map[string]interface{}{"a": "b"}
	relations := []Relation{Eq("foo", "bar")}
	keys := Keys{PartitionKeys: []string{"foo"}}
	stmt, err := NewUpdateStatement("ks1", "tbl1", fieldMap, relations, keys)
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE ks1.tbl1 SET a = ? WHERE foo = ?", stmt.Query())
	assert.Equal(t, []interface{}{"b", "bar"}, stmt.Values())

	fieldMap = map[string]interface{}{"a": "b", "c": ListAppend("d")}
	stmt, err = NewUpdateStatement("ks1", "tbl1", fieldMap, relations, keys)
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE ks1.tbl1 SET a = ?, c = c + ? WHERE foo = ?", stmt.Query())
	assert.Equal(t, []interface{}{"b", []interface{}{"d"}, "bar"}, stmt.Values())

	fieldMap = map[string]interface{}{"a": "b", "c": "d"}
	stmt, err = NewUpdateStatement("ks1", "tbl1", fieldMap, relations, keys)
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE ks1.tbl1 SET a = ?, c = ? WHERE foo = ?", stmt.Query())
	assert.Equal(t, []interface{}{"b", "d", "bar"}, stmt.Values())

	relations = []Relation{
		Eq("foo", "bar"),
		In("baz", "a", "b", "c"),
	}
	stmt, err = NewUpdateStatement("ks1", "tbl1", fieldMap, relations, keys)
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE ks1.tbl1 SET a = ?, c = ? WHERE foo = ? AND baz IN ?", stmt.Query())
	assert.Equal(t, []interface{}{"b", "d", "bar", []interface{}{"a", "b", "c"}}, stmt.Values())

	stmt = stmt.WithTTL(1 * time.Hour)
	assert.Equal(t, "UPDATE ks1.tbl1 USING TTL ? SET a = ?, c = ? WHERE foo = ? AND baz IN ?", stmt.Query())
	assert.Equal(t, []interface{}{3600, "b", "d", "bar", []interface{}{"a", "b", "c"}}, stmt.Values())
}

func TestDeleteStatement(t *testing.T) {
	keys := Keys{PartitionKeys: []string{"foo"}}
	relations := []Relation{Eq("foo", "bar")}
	stmt, err := NewDeleteStatement("ks1", "tbl1", relations, keys)
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM ks1.tbl1 WHERE foo = ?", stmt.Query())
	assert.Equal(t, []interface{}{"bar"}, stmt.Values())

	relations = []Relation{
		Eq("foo", "bar"),
		In("baz", "a", "b", "c"),
	}
	stmt, err = NewDeleteStatement("ks1", "tbl1", relations, keys)
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM ks1.tbl1 WHERE foo = ? AND baz IN ?", stmt.Query())
	assert.Equal(t, []interface{}{"bar", []interface{}{"a", "b", "c"}}, stmt.Values())
}

func TestStatementsWithSentinel(t *testing.T) {
	t.Run("SelectStatement", func(t *testing.T) {
		fields := []string{"a", "b", "c"}
		keys := Keys{PartitionKeys: []string{"a"}, ClusteringColumns: []string{"b"}}
		stmt, err := NewSelectStatement("ks1", "tbl1", fields, nil, keys)
		require.NoError(t, err)

		stmt = stmt.WithRelations([]Relation{
			Eq("a", "hello"),
			Eq("b", ""),
		})
		assert.Equal(t, "SELECT a, b, c FROM ks1.tbl1 WHERE a = ? AND b = ?", stmt.Query())
		assert.Equal(t, []interface{}{"hello", ""}, stmt.Values())

		stmt = stmt.WithRelations([]Relation{
			Eq("a", ""),
			Eq("b", ""),
		})
		stmt = stmt.WithClusteringSentinel(true)
		assert.Equal(t, "SELECT a, b, c FROM ks1.tbl1 WHERE a = ? AND b = ?", stmt.Query())
		assert.Equal(t, []interface{}{"", ClusteringSentinel}, stmt.Values())
	})

	t.Run("InsertStatement", func(t *testing.T) {
		fieldMap := map[string]interface{}{"a": "", "b": "", "c": ""}
		keys := Keys{PartitionKeys: []string{"a"}, ClusteringColumns: []string{"b"}}

		stmt, err := NewInsertStatement("ks1", "tbl1", fieldMap, keys)
		assert.NoError(t, err)
		assert.Equal(t, "INSERT INTO ks1.tbl1 (a, b, c) VALUES (?, ?, ?)", stmt.Query())
		assert.Equal(t, []interface{}{"", "", ""}, stmt.Values())

		stmt = stmt.WithClusteringSentinel(true)
		assert.Equal(t, "INSERT INTO ks1.tbl1 (a, b, c) VALUES (?, ?, ?)", stmt.Query())
		assert.Equal(t, []interface{}{"", ClusteringSentinel, ""}, stmt.Values())
	})

	t.Run("UpdateStatement", func(t *testing.T) {
		fieldMap := map[string]interface{}{"c": ""}
		keys := Keys{PartitionKeys: []string{"a"}, ClusteringColumns: []string{"b"}}
		relations := []Relation{
			Eq("a", ""),
			Eq("b", ""),
		}

		stmt, err := NewUpdateStatement("ks1", "tbl1", fieldMap, relations, keys)
		assert.NoError(t, err)
		assert.Equal(t, "UPDATE ks1.tbl1 SET c = ? WHERE a = ? AND b = ?", stmt.Query())
		assert.Equal(t, []interface{}{"", "", ""}, stmt.Values())

		stmt = stmt.WithClusteringSentinel(true)
		assert.Equal(t, "UPDATE ks1.tbl1 SET c = ? WHERE a = ? AND b = ?", stmt.Query())
		assert.Equal(t, []interface{}{"", "", ClusteringSentinel}, stmt.Values())
	})

	t.Run("DeleteStatement", func(t *testing.T) {
		keys := Keys{PartitionKeys: []string{"a"}, ClusteringColumns: []string{"b"}}
		relations := []Relation{
			Eq("a", ""),
			Eq("b", ""),
		}

		stmt, err := NewDeleteStatement("ks1", "tbl1", relations, keys)
		assert.NoError(t, err)
		assert.Equal(t, "DELETE FROM ks1.tbl1 WHERE a = ? AND b = ?", stmt.Query())
		assert.Equal(t, []interface{}{"", ""}, stmt.Values())

		stmt = stmt.WithClusteringSentinel(true)
		assert.Equal(t, "DELETE FROM ks1.tbl1 WHERE a = ? AND b = ?", stmt.Query())
		assert.Equal(t, []interface{}{"", ClusteringSentinel}, stmt.Values())
	})
}

func TestGenerateWhereCQL(t *testing.T) {
	stmt, values := generateWhereCQL([]Relation{
		Eq("foo", "bar"),
	}, Keys{}, false)
	assert.Equal(t, "foo = ?", stmt)
	assert.Equal(t, []interface{}{"bar"}, values)

	stmt, values = generateWhereCQL([]Relation{
		Eq("foo", "bar"),
		In("baz", "a", "b", "c"),
	}, Keys{}, false)
	assert.Equal(t, "foo = ? AND baz IN ?", stmt)
	assert.Equal(t, []interface{}{"bar", []interface{}{"a", "b", "c"}}, values)

	stmt, values = generateWhereCQL([]Relation{
		Eq("foo", "bar"),
	}, Keys{ClusteringColumns: []string{"foo"}}, true)
	assert.Equal(t, "foo = ?", stmt)
	assert.Equal(t, []interface{}{"bar"}, values)

	stmt, values = generateWhereCQL([]Relation{
		Eq("foo", ""),
	}, Keys{ClusteringColumns: []string{"foo"}}, true)
	assert.Equal(t, "foo = ?", stmt)
	assert.Equal(t, []interface{}{ClusteringSentinel}, values)

	stmt, values = generateWhereCQL([]Relation{
		Eq("bar", ""),
	}, Keys{ClusteringColumns: []string{"foo"}}, true)
	assert.Equal(t, "bar = ?", stmt)
	assert.Equal(t, []interface{}{""}, values)
}

func TestGenerateRelationCQL(t *testing.T) {
	stmt, value := generateRelationCQL(Eq("foo", "bar"), Keys{}, false)
	assert.Equal(t, "foo = ?", stmt)
	assert.Equal(t, "bar", value)

	stmt, value = generateRelationCQL(Eq("FoO", "BAR"), Keys{}, false)
	assert.Equal(t, "foo = ?", stmt)
	assert.Equal(t, "BAR", value)

	stmt, value = generateRelationCQL(Eq("foo", ""),
		Keys{ClusteringColumns: []string{"foo"}}, true)
	assert.Equal(t, "foo = ?", stmt)
	assert.Equal(t, ClusteringSentinel, value)

	stmt, value = generateRelationCQL(Eq("FoO", ""),
		Keys{ClusteringColumns: []string{"foo"}}, true)
	assert.Equal(t, "foo = ?", stmt)
	assert.Equal(t, ClusteringSentinel, value)

	stmt, value = generateRelationCQL(Eq("FoO", 0),
		Keys{ClusteringColumns: []string{"foo"}}, true)
	assert.Equal(t, "foo = ?", stmt)
	assert.Equal(t, 0, value)

	stmt, value = generateRelationCQL(In("foo", "a", "b", "c"), Keys{}, false)
	assert.Equal(t, "foo IN ?", stmt)
	assert.Equal(t, []interface{}{"a", "b", "c"}, value)

	stmt, value = generateRelationCQL(GT("foo", 1), Keys{}, false)
	assert.Equal(t, "foo > ?", stmt)
	assert.Equal(t, 1, value)

	stmt, value = generateRelationCQL(GTE("foo", 1), Keys{}, false)
	assert.Equal(t, "foo >= ?", stmt)
	assert.Equal(t, 1, value)

	stmt, value = generateRelationCQL(LT("foo", 1), Keys{}, false)
	assert.Equal(t, "foo < ?", stmt)
	assert.Equal(t, 1, value)

	stmt, value = generateRelationCQL(LTE("foo", 1), Keys{}, false)
	assert.Equal(t, "foo <= ?", stmt)
	assert.Equal(t, 1, value)

	assert.PanicsWithValue(t, "unknown comparator -1", func() {
		stmt, value = generateRelationCQL(Relation{cmp: -1}, Keys{}, false)
	})
}

func TestGenerateOrderByCQL(t *testing.T) {
	stmt := generateOrderByCQL([]ClusteringOrderColumn{})
	assert.Equal(t, "", stmt)

	stmt = generateOrderByCQL([]ClusteringOrderColumn{
		{Column: "foo", Direction: ASC},
	})
	assert.Equal(t, "foo ASC", stmt)

	stmt = generateOrderByCQL([]ClusteringOrderColumn{
		{Column: "foo", Direction: ASC},
		{Column: "bar", Direction: DESC},
	})
	assert.Equal(t, "foo ASC, bar DESC", stmt)
}

func TestClusteringFieldOrSentinel(t *testing.T) {
	assert.Equal(t, ClusteringSentinel, ClusteringFieldOrSentinel(""))
	assert.Equal(t, "foo", ClusteringFieldOrSentinel("foo"))

	assert.Equal(t, []byte(ClusteringSentinel), ClusteringFieldOrSentinel([]byte{}))
	assert.Equal(t, []byte{0x00}, ClusteringFieldOrSentinel([]byte{0x00}))

	assert.Equal(t, ClusteringSentinelTimestamp, ClusteringFieldOrSentinel(time.Time{}))
	assert.Equal(t, time.Unix(10, 0), ClusteringFieldOrSentinel(time.Unix(10, 0)))

	assert.Equal(t, 0, ClusteringFieldOrSentinel(0))
	assert.Equal(t, 42, ClusteringFieldOrSentinel(42))
	assert.Equal(t, struct{}{}, ClusteringFieldOrSentinel(struct{}{}))
}

func TestIsClusteringSentinelValue(t *testing.T) {
	type fooString string
	type fooSlice []byte
	type fooTime time.Time

	testCases := []struct {
		desc               string
		input              interface{}
		expectedIsSentinel bool
		expectedOutput     interface{}
	}{
		{
			desc:               "sentinel string",
			input:              ClusteringSentinel,
			expectedIsSentinel: true,
			expectedOutput:     "",
		},
		{
			desc:               "indirect sentinel string",
			input:              fooString(ClusteringSentinel),
			expectedIsSentinel: true,
			expectedOutput:     fooString(""),
		},
		{
			desc:               "empty string",
			input:              "",
			expectedIsSentinel: false,
			expectedOutput:     "",
		},
		{
			desc:               "other string",
			input:              "foo",
			expectedIsSentinel: false,
			expectedOutput:     "foo",
		},
		{
			desc:               "sentinel slice",
			input:              []byte(ClusteringSentinel),
			expectedIsSentinel: true,
			expectedOutput:     []byte{},
		},
		{
			desc:               "indirect sentinel slice",
			input:              fooSlice(ClusteringSentinel),
			expectedIsSentinel: true,
			expectedOutput:     fooSlice{},
		},
		{
			desc:               "nil slice",
			input:              []byte(nil),
			expectedIsSentinel: false,
			expectedOutput:     []byte(nil),
		},
		{
			desc:               "empty byte slice",
			input:              []byte{},
			expectedIsSentinel: false,
			expectedOutput:     []byte{},
		},
		{
			desc:               "other byte slice",
			input:              []byte{0x00},
			expectedIsSentinel: false,
			expectedOutput:     []byte{0x00},
		},
		{
			desc:               "int zero",
			input:              0,
			expectedIsSentinel: false,
			expectedOutput:     0,
		},
		{
			desc:               "int 42",
			input:              42,
			expectedIsSentinel: false,
			expectedOutput:     42,
		},
		{
			desc:               "time struct",
			input:              time.Time{},
			expectedIsSentinel: false,
			expectedOutput:     time.Time{},
		},
		{
			desc:               "time struct with value",
			input:              time.Unix(10, 0),
			expectedIsSentinel: false,
			expectedOutput:     time.Unix(10, 0),
		},
		{
			desc:               "sentinel time struct",
			input:              ClusteringSentinelTimestamp,
			expectedIsSentinel: true,
			expectedOutput:     time.Time{},
		},
		{
			desc:               "indirect time struct",
			input:              fooTime{},
			expectedIsSentinel: false,
			expectedOutput:     fooTime{},
		},
		{
			desc:               "indirect time struct with value",
			input:              fooTime(time.Unix(10, 0)),
			expectedIsSentinel: false,
			expectedOutput:     fooTime(time.Unix(10, 0)),
		},
		{
			desc:               "indirect sentinel time struct",
			input:              fooTime(ClusteringSentinelTimestamp),
			expectedIsSentinel: true,
			expectedOutput:     fooTime{},
		},
		{
			desc:               "empty struct",
			input:              struct{}{},
			expectedIsSentinel: false,
			expectedOutput:     struct{}{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			isSentinel, nonSentinelVal := IsClusteringSentinelValue(tc.input)
			assert.Equal(t, tc.expectedIsSentinel, isSentinel)
			assert.Equal(t, tc.expectedOutput, nonSentinelVal)
		})
	}
}
