package gocassa

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"time"
)

var (
	// ClusteringSentinel represents a placeholder value to be used for cases
	// where a value needs to be present (ie: a stand-in representing a
	// clustering key that is empty)
	ClusteringSentinel = "<gocassa.ClusteringSentinel>"

	// ClusteringSentinelTimestamp represents a placeholder time value for
	// cases where we have a null timestamp column. We've chosen the time
	// 1753-01-01 which ironically is the minimum date in SQL Server
	ClusteringSentinelTimestamp = time.Unix(-6847804725, 0)

	// timeReflectType represents the reflect.Type of time.Time (putting
	// this here allows us to optimise allocations)
	timeReflectType = reflect.TypeOf(time.Time{})
)

// SelectStatement represents a read (SELECT) query for some data in C*
// It satisfies the Statement interface
type SelectStatement struct {
	keyspace                   string                  // name of the keyspace
	table                      string                  // name of the table
	fields                     []string                // list of fields we want to select
	where                      []Relation              // where filter clauses
	order                      []ClusteringOrderColumn // order by clauses
	limit                      int                     // limit count, 0 means no limit
	allowFiltering             bool                    // whether we should allow filtering
	keys                       Keys                    // partition / clustering keys for table
	clusteringSentinelsEnabled bool                    // whether we should enable our clustering sentinel
}

// NewSelectStatement adds the ability to craft a new SelectStatement
// This function will error if the parameters passed in are invalid
func NewSelectStatement(keyspace, table string, fields []string, rel []Relation, keys Keys) (SelectStatement, error) {
	stmt := SelectStatement{}
	if keyspace == "" || table == "" {
		return stmt, fmt.Errorf("keyspace and table can't be empty")
	}

	if len(fields) < 1 {
		return stmt, fmt.Errorf("fields must be a list of string fields to select")
	}

	if len(keys.PartitionKeys) == 0 {
		return stmt, fmt.Errorf("partition key should be supplied")
	}

	stmt.keyspace = keyspace
	stmt.table = table
	stmt.fields = fields
	stmt.where = rel
	stmt.keys = keys
	return stmt, nil
}

// Query provides the CQL query string for an SELECT query
func (s SelectStatement) Query() string {
	query, _ := s.QueryAndValues()
	return query
}

// Values provide the binding values for an SELECT query
func (s SelectStatement) Values() []interface{} {
	_, values := s.QueryAndValues()
	return values
}

// QueryAndValues returns the CQL query and any bind values
func (s SelectStatement) QueryAndValues() (string, []interface{}) {
	values := make([]interface{}, 0)
	query := []string{
		"SELECT",
		strings.Join(s.fields, ", "),
		fmt.Sprintf("FROM %s.%s", s.Keyspace(), s.Table()),
	}

	whereCQL, whereValues := generateWhereCQL(s.Relations(), s.Keys(), s.clusteringSentinelsEnabled)
	if whereCQL != "" {
		query = append(query, "WHERE", whereCQL)
		values = append(values, whereValues...)
	}

	orderByCQL := generateOrderByCQL(s.OrderBy())
	if orderByCQL != "" {
		query = append(query, "ORDER BY", orderByCQL)
	}

	if s.Limit() > 0 {
		query = append(query, "LIMIT ?")
		values = append(values, s.limit)
	}

	if s.AllowFiltering() {
		query = append(query, "ALLOW FILTERING")
	}

	return strings.Join(query, " "), values
}

// Keyspace returns the name of the Keyspace for the statement
func (s SelectStatement) Keyspace() string {
	return s.keyspace
}

// Table returns the name of the table for this statement
func (s SelectStatement) Table() string {
	return s.table
}

// Fields returns the list of fields to be selected
func (s SelectStatement) Fields() []string {
	return s.fields
}

// Relations provides the WHERE clause Relation items used to evaluate
// this query
func (s SelectStatement) Relations() []Relation {
	return s.where
}

// WithRelations sets the relations (WHERE conditions) for this statement
func (s SelectStatement) WithRelations(rel []Relation) SelectStatement {
	s.where = rel
	return s
}

// OrderBy returns the ClusteringOrderColumn clauses used
func (s SelectStatement) OrderBy() []ClusteringOrderColumn {
	return s.order
}

// WithOrderBy allows the setting of the clustering order columns
func (s SelectStatement) WithOrderBy(order []ClusteringOrderColumn) SelectStatement {
	s.order = order
	return s
}

// Limit returns the number of rows to be returned, a value of zero
// means no limit
func (s SelectStatement) Limit() int {
	if s.limit < 1 {
		return 0
	}
	return s.limit
}

// WithLimit allows the setting of a limit. Using a value of zero or a negative
// value removes the limit
func (s SelectStatement) WithLimit(limit int) SelectStatement {
	if limit < 1 {
		limit = 0
	}
	s.limit = limit
	return s
}

// AllowFiltering returns whether data filtering (ALLOW FILTERING) is enabled
func (s SelectStatement) AllowFiltering() bool {
	return s.allowFiltering
}

// WithAllowFiltering allows toggling of data filtering (including
// ALLOW FILTERING in the CQL)
func (s SelectStatement) WithAllowFiltering(enabled bool) SelectStatement {
	s.allowFiltering = enabled
	return s
}

// Keys provides the Partition / Clustering keys defined by the table recipe
func (s SelectStatement) Keys() Keys {
	return s.keys
}

// WithClusteringSentinel allows you to specify whether the use of the
// clustering sentinel value is enabled
func (s SelectStatement) WithClusteringSentinel(enabled bool) SelectStatement {
	s.clusteringSentinelsEnabled = enabled
	return s
}

// InsertStatement represents an INSERT query to write some data in C*
// It satisfies the Statement interface
type InsertStatement struct {
	keyspace             string                 // name of the keyspace
	table                string                 // name of the table
	fieldMap             map[string]interface{} // fields to be inserted
	ttl                  time.Duration          // ttl of the row
	ifNotExists          bool                   // use IF NOT EXISTS/Lightweight Transactions
	keys                 Keys                   // partition / clustering keys for table
	allowClusterSentinel bool                   // whether we should enable our clustering sentinel
}

// NewInsertStatement adds the ability to craft a new InsertStatement
// This function will error if the parameters passed in are invalid
func NewInsertStatement(keyspace, table string, fieldMap map[string]interface{}, keys Keys) (InsertStatement, error) {
	stmt := InsertStatement{}
	if keyspace == "" || table == "" {
		return stmt, fmt.Errorf("keyspace and table can't be empty")
	}

	if len(fieldMap) < 1 {
		return stmt, fmt.Errorf("fieldMap must be a map fields to insert")
	}

	if len(keys.PartitionKeys) == 0 {
		return stmt, fmt.Errorf("partition key should be supplied")
	}

	stmt.keyspace = keyspace
	stmt.table = table
	stmt.fieldMap = fieldMap
	stmt.keys = keys
	return stmt, nil
}

// Query provides the CQL query string for an INSERT INTO query
func (s InsertStatement) Query() string {
	query, _ := s.QueryAndValues()
	return query
}

// Values provide the binding values for an INSERT INTO query
func (s InsertStatement) Values() []interface{} {
	_, values := s.QueryAndValues()
	return values
}

// QueryAndValues returns the CQL query and any bind values
func (s InsertStatement) QueryAndValues() (string, []interface{}) {
	query := []string{"INSERT INTO", fmt.Sprintf("%s.%s", s.Keyspace(), s.Table())}

	fieldMap := s.FieldMap()
	fieldNames := make([]string, 0, len(fieldMap))
	placeholders := make([]string, 0, len(fieldMap))
	values := make([]interface{}, 0, len(fieldMap))
	for _, field := range sortedKeys(fieldMap) {
		fieldNames = append(fieldNames, strings.ToLower(field))
		placeholders = append(placeholders, "?")
		if isClusteringKeyField(field, s.keys) && s.allowClusterSentinel {
			values = append(values, ClusteringFieldOrSentinel(fieldMap[field]))
		} else {
			values = append(values, fieldMap[field])
		}
	}

	query = append(query, "("+strings.Join(fieldNames, ", ")+")")
	query = append(query, "VALUES ("+strings.Join(placeholders, ", ")+")")

	// Determine if we need to set a TTL
	if s.TTL() > time.Duration(0) {
		query = append(query, "USING TTL ?")
		values = append(values, int(s.TTL().Seconds()))
	}

	// Set IF NOT EXISTS
	if s.IfNotExists() {
		query = append(query, "IF NOT EXISTS")
	}

	return strings.Join(query, " "), values
}

// Keyspace returns the name of the Keyspace for the statement
func (s InsertStatement) Keyspace() string {
	return s.keyspace
}

// Table returns the name of the table for this statement
func (s InsertStatement) Table() string {
	return s.table
}

// FieldMap gives a map of all the fields to be inserted. In an INSERT
// statement, none of these will be Modifier types
func (s InsertStatement) FieldMap() map[string]interface{} {
	return s.fieldMap
}

// TTL returns the Time-To-Live for this row statement. A duration of 0
// means there is no TTL
func (s InsertStatement) TTL() time.Duration {
	if s.ttl < time.Duration(1) {
		return time.Duration(0)
	}
	return s.ttl
}

// WithTTL allows setting of the time-to-live for this insert statement.
// A duration of 0 means there is no TTL
func (s InsertStatement) WithTTL(ttl time.Duration) InsertStatement {
	if ttl < time.Duration(1) {
		ttl = time.Duration(0)
	}
	s.ttl = ttl
	return s
}

// IfNotExists returns if the statement should be applied with IF NOT EXISTS (Lightweight Transactions)
func (s InsertStatement) IfNotExists() bool {
	return s.ifNotExists
}

// WithTTL allows setting of the time-to-live for this insert statement.
// A duration of 0 means there is no TTL
func (s InsertStatement) WithIfNotExists() InsertStatement {
	s.ifNotExists = true
	return s
}

// Keys provides the Partition / Clustering keys defined by the table recipe
func (s InsertStatement) Keys() Keys {
	return s.keys
}

// WithClusteringSentinel allows you to specify whether the use of the
// clustering sentinel value is enabled
func (s InsertStatement) WithClusteringSentinel(enabled bool) InsertStatement {
	s.allowClusterSentinel = enabled
	return s
}

// UpdateStatement represents an UPDATE query to update some data in C*
// It satisfies the Statement interface
type UpdateStatement struct {
	keyspace             string                 // name of the keyspace
	table                string                 // name of the table
	fieldMap             map[string]interface{} // fields to be updated
	where                []Relation             // where filter clauses
	ttl                  time.Duration          // ttl of the row
	keys                 Keys                   // partition / clustering keys for table
	allowClusterSentinel bool                   // whether we should enable our clustering sentinel
}

// NewUpdateStatement adds the ability to craft a new UpdateStatement
// This function will error if the parameters passed in are invalid
func NewUpdateStatement(keyspace, table string, fieldMap map[string]interface{}, rel []Relation, keys Keys) (UpdateStatement, error) {
	stmt := UpdateStatement{}
	if keyspace == "" || table == "" {
		return stmt, fmt.Errorf("keyspace and table can't be empty")
	}

	if len(fieldMap) < 1 {
		return stmt, fmt.Errorf("fieldMap must be a map fields to insert")
	}

	if len(rel) < 1 {
		return stmt, fmt.Errorf("must supply at least one relation WHERE clause")
	}

	if len(keys.PartitionKeys) == 0 {
		return stmt, fmt.Errorf("partition key should be supplied")
	}

	stmt.keyspace = keyspace
	stmt.table = table
	stmt.fieldMap = fieldMap
	stmt.where = rel
	stmt.keys = keys
	return stmt, nil
}

// Query provides the CQL query string for an UPDATE query
func (s UpdateStatement) Query() string {
	query, _ := s.QueryAndValues()
	return query
}

// Values provide the binding values for an UPDATE query
func (s UpdateStatement) Values() []interface{} {
	_, values := s.QueryAndValues()
	return values
}

// QueryAndValues returns the CQL query and any bind values
func (s UpdateStatement) QueryAndValues() (string, []interface{}) {
	values := make([]interface{}, 0)
	query := []string{"UPDATE", fmt.Sprintf("%s.%s", s.Keyspace(), s.Table())}

	// Determine if we need to set a TTL
	if s.TTL() > 0 {
		query = append(query, "USING TTL ?")
		values = append(values, int(s.TTL().Seconds()))
	}

	setCQL, setValues := generateUpdateSetCQL(s.FieldMap())
	query = append(query, "SET", setCQL)
	values = append(values, setValues...)

	whereCQL, whereValues := generateWhereCQL(s.Relations(), s.Keys(), s.allowClusterSentinel)
	if whereCQL != "" {
		query = append(query, "WHERE", whereCQL)
		values = append(values, whereValues...)
	}
	return strings.Join(query, " "), values
}

// Keyspace returns the name of the Keyspace for the statement
func (s UpdateStatement) Keyspace() string {
	return s.keyspace
}

// Table returns the name of the table for this statement
func (s UpdateStatement) Table() string {
	return s.table
}

// FieldMap gives a map of all the fields to be inserted. In an UPDATE
// statement, the values may be Modifier types
func (s UpdateStatement) FieldMap() map[string]interface{} {
	return s.fieldMap
}

// Relations provides the WHERE clause Relation items used to evaluate
// this query
func (s UpdateStatement) Relations() []Relation {
	return s.where
}

// TTL returns the Time-To-Live for this row statement. A duration of 0
// means there is no TTL
func (s UpdateStatement) TTL() time.Duration {
	if s.ttl < time.Duration(1) {
		return time.Duration(0)
	}
	return s.ttl
}

// WithTTL allows setting of the time-to-live for this insert statement.
// A duration of 0 means there is no TTL
func (s UpdateStatement) WithTTL(ttl time.Duration) UpdateStatement {
	if ttl < time.Duration(1) {
		ttl = time.Duration(0)
	}
	s.ttl = ttl
	return s
}

// Keys provides the Partition / Clustering keys defined by the table recipe
func (s UpdateStatement) Keys() Keys {
	return s.keys
}

// WithClusteringSentinel allows you to specify whether the use of the
// clustering sentinel value is enabled
func (s UpdateStatement) WithClusteringSentinel(enabled bool) UpdateStatement {
	s.allowClusterSentinel = enabled
	return s
}

// DeleteStatement represents a DELETE query to delete some data in C*
// It satisfies the Statement interface
type DeleteStatement struct {
	keyspace             string     // name of the keyspace
	table                string     // name of the table
	where                []Relation // where filter clauses
	keys                 Keys       // partition / clustering keys for table
	allowClusterSentinel bool       // whether we should enable our clustering sentinel
}

// NewDeleteStatement adds the ability to craft a new DeleteStatement
// This function will error if the parameters passed in are invalid
func NewDeleteStatement(keyspace, table string, rel []Relation, keys Keys) (DeleteStatement, error) {
	stmt := DeleteStatement{}
	if keyspace == "" || table == "" {
		return stmt, fmt.Errorf("keyspace and table can't be empty")
	}

	if len(rel) < 1 {
		return stmt, fmt.Errorf("must supply at least one relation WHERE clause")
	}

	if len(keys.PartitionKeys) == 0 {
		return stmt, fmt.Errorf("partition key should be supplied")
	}

	stmt.keyspace = keyspace
	stmt.table = table
	stmt.where = rel
	stmt.keys = keys
	return stmt, nil
}

// Query provides the CQL query string for a DELETE query
func (s DeleteStatement) Query() string {
	query, _ := s.QueryAndValues()
	return query
}

// Values provide the binding values for a DELETE query
func (s DeleteStatement) Values() []interface{} {
	_, values := s.QueryAndValues()
	return values
}

// QueryAndValues returns the CQL query and any bind values
func (s DeleteStatement) QueryAndValues() (string, []interface{}) {
	query := fmt.Sprintf("DELETE FROM %s.%s", s.Keyspace(), s.Table())
	whereCQL, whereValues := generateWhereCQL(s.Relations(), s.Keys(), s.allowClusterSentinel)
	if whereCQL != "" {
		query += " WHERE " + whereCQL
	}
	return query, whereValues
}

// Keyspace returns the name of the Keyspace for the statement
func (s DeleteStatement) Keyspace() string {
	return s.keyspace
}

// Table returns the name of the table for this statement
func (s DeleteStatement) Table() string {
	return s.table
}

// Relations provides the WHERE clause Relation items used to evaluate
// this query
func (s DeleteStatement) Relations() []Relation {
	return s.where
}

// Keys provides the Partition / Clustering keys defined by the table recipe
func (s DeleteStatement) Keys() Keys {
	return s.keys
}

// WithClusteringSentinel allows you to specify whether the use of the
// clustering sentinel value is enabled
func (s DeleteStatement) WithClusteringSentinel(enabled bool) DeleteStatement {
	s.allowClusterSentinel = enabled
	return s
}

// cqlStatement represents a statement that executes raw CQL
type cqlStatement struct {
	query  string
	values []interface{}
}

func (s cqlStatement) Query() string { return s.query }

func (s cqlStatement) Values() []interface{} { return s.values }

// noOpStatement represents a statement that doesn't perform any specific
// query. It's used internally for testing, satisfies the Statement interface
type noOpStatement struct{}

func (_ noOpStatement) Query() string { return "" }

func (_ noOpStatement) Values() []interface{} { return []interface{}{} }

// generateUpdateSetCQL takes in a field map and generates the comma separated
// SET syntax. An expected output may be something like:
//   - "foo = ?", {1}
//   - "foo = ?, bar = ?", {1, 2}
func generateUpdateSetCQL(fm map[string]interface{}) (string, []interface{}) {
	clauses, values := make([]string, 0, len(fm)), make([]interface{}, 0, len(fm))
	for _, fieldName := range sortedKeys(fm) {
		value := fm[fieldName]
		if modifier, ok := value.(Modifier); ok {
			stmt, vals := modifier.cql(fieldName)
			clauses = append(clauses, stmt)
			values = append(values, vals...)
			continue
		}
		clauses = append(clauses, fieldName+" = ?")
		values = append(values, value)
	}
	return strings.Join(clauses, ", "), values
}

// generateWhereCQL takes a list of relations and generates the CQL for
// a WHERE clause. An expected output may be something like:
//   - "foo = ?", {1}
//   - "foo = ? AND bar IN ?", {1, {"a", "b", "c"}}
func generateWhereCQL(rs []Relation, keys Keys, clusteringSentinelsEnabled bool) (string, []interface{}) {
	clauses, values := make([]string, 0, len(rs)), make([]interface{}, 0, len(rs))
	for _, relation := range rs {
		clause, bindValue := generateRelationCQL(relation, keys, clusteringSentinelsEnabled)
		clauses = append(clauses, clause)
		values = append(values, bindValue)
	}
	return strings.Join(clauses, " AND "), values
}

func generateRelationCQL(rel Relation, keys Keys, clusteringSentinelsEnabled bool) (string, interface{}) {
	field := strings.ToLower(rel.Field())
	switch rel.Comparator() {
	case CmpEquality:
		if isClusteringKeyField(rel.Field(), keys) && clusteringSentinelsEnabled {
			return field + " = ?", ClusteringFieldOrSentinel(rel.Terms()[0])
		}
		return field + " = ?", rel.Terms()[0]
	case CmpIn:
		return field + " IN ?", rel.Terms()
	case CmpGreaterThan:
		return field + " > ?", rel.Terms()[0]
	case CmpGreaterThanOrEquals:
		return field + " >= ?", rel.Terms()[0]
	case CmpLesserThan:
		return field + " < ?", rel.Terms()[0]
	case CmpLesserThanOrEquals:
		return field + " <= ?", rel.Terms()[0]
	default:
		// This represents an invalid Comparator and would only manifest
		// if we've initialised a Relation incorrectly within this package
		panic(fmt.Sprintf("unknown comparator %v", rel.Comparator()))
	}
}

// generateOrderByCQL generates the CQL for the ORDER BY clause. An expected
// output might look like:
//   - foo ASC
//   - foo ASC, bar DESC
func generateOrderByCQL(order []ClusteringOrderColumn) string {
	out := make([]string, 0, len(order))
	for _, oc := range order {
		out = append(out, oc.Column+" "+oc.Direction.String())
	}
	return strings.Join(out, ", ")
}

// isClusteringKeyField determines whether the relation makes up the
// clustering key of the statement
func isClusteringKeyField(field string, keys Keys) bool {
	for _, key := range keys.ClusteringColumns {
		if strings.ToLower(key) == strings.ToLower(field) {
			return true
		}
	}
	return false
}

// ClusteringFieldOrSentinel will check if we should substitute in our
// sentinel value for empty clustering fields
func ClusteringFieldOrSentinel(term interface{}) interface{} {
	switch v := term.(type) {
	case string:
		if len(v) == 0 {
			return ClusteringSentinel
		}
		return v
	case []byte:
		if len(v) == 0 {
			return []byte(ClusteringSentinel)
		}
		return v
	case time.Time:
		if v.IsZero() {
			return ClusteringSentinelTimestamp
		}
		return v
	default:
		return term
	}
}

// IsClusteringSentinelValue returns a boolean on whether the value passed in
// is the clustering sentinel value and what the non-sentinel value is
func IsClusteringSentinelValue(term interface{}) (bool, interface{}) {
	val := reflect.ValueOf(term)
	switch {
	case val.Kind() == reflect.String:
		if val.String() == ClusteringSentinel {
			return true, reflect.New(val.Type()).Elem().Interface()
		}
		return false, term
	case val.Kind() == reflect.Slice && val.Type().Elem().Kind() == reflect.Uint8:
		if bytes.Equal(val.Bytes(), []byte(ClusteringSentinel)) {
			return true, reflect.MakeSlice(val.Type(), 0, 0).Interface()
		}
		return false, term
	case val.Kind() == reflect.Struct:
		if val.Type().ConvertibleTo(timeReflectType) {
			convertedTerm := val.Convert(timeReflectType).Interface().(time.Time)
			if convertedTerm.Equal(ClusteringSentinelTimestamp) {
				return true, reflect.New(val.Type()).Elem().Interface()
			}
		}
		return false, term
	default:
		return false, term
	}
}
