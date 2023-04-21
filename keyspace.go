package gocassa

import (
	"fmt"
	"strings"
	"time"
)

type tableFactory interface {
	NewTable(string, interface{}, map[string]interface{}, Keys) Table
}

type k struct {
	qe           QueryExecutor
	name         string
	debugMode    bool
	tableFactory tableFactory
}

// Connect to a certain keyspace directly. Same as using Connect().KeySpace(keySpaceName)
func ConnectToKeySpace(keySpace string, nodeIps []string, username, password string) (KeySpace, error) {
	c, err := Connect(nodeIps, username, password)
	if err != nil {
		return nil, err
	}
	return c.KeySpace(keySpace), nil
}

func (k *k) DebugMode(b bool) {
	k.debugMode = b
}

func (k *k) Table(tableNamePrefix string, entity interface{}, keys Keys) Table {
	n := tableNamePrefix + "__" + strings.Join(keys.PartitionKeys, "_") + "__" + strings.Join(keys.ClusteringColumns, "_")
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}
	return k.NewTable(n, entity, m, keys)
}

func (k *k) NewTable(name string, entity interface{}, fields map[string]interface{}, keys Keys) Table {
	// Act both as a proxy to a tableFactory, and as the tableFactory itself (in most situations, a k will be its own
	// tableFactory, but not always [ie. mocking])
	if k.tableFactory != k {
		return k.tableFactory.NewTable(name, entity, fields, keys)
	} else {
		ti := newTableInfo(k.name, name, keys, entity, fields)
		return &table{
			keySpace: k,
			info:     ti,
			options:  Options{},
		}
	}
}

func (k *k) MapTable(tableNamePrefix, partitionKeyField string, entity interface{}) MapTable {
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}
	return &mapT{
		table: k.NewTable(fmt.Sprintf("%s_map_%s", tableNamePrefix, partitionKeyField), entity, m, Keys{
			PartitionKeys: []string{partitionKeyField},
		}),
		partitionKeyField: partitionKeyField,
	}
}

func (k *k) SetKeysSpaceName(name string) {
	k.name = name
}

func (k *k) MultimapTable(tableNamePrefix, partitionKeyField, clusteringKeyField string, entity interface{}) MultimapTable {
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}
	return &multimapT{
		table: k.NewTable(fmt.Sprintf("%s_multimap_%s_%s", tableNamePrefix, partitionKeyField, clusteringKeyField), entity, m, Keys{
			PartitionKeys:     []string{partitionKeyField},
			ClusteringColumns: []string{clusteringKeyField},
		}),
		clusteringKeyField: clusteringKeyField,
		partitionKeyField:  partitionKeyField,
	}
}

func (k *k) MultimapMultiKeyTable(tableNamePrefix string, partitionKeyFields, clusteringKeyFields []string, entity interface{}) MultimapMkTable {
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}
	return &multimapMkT{
		table: k.NewTable(fmt.Sprintf("%s_multimapMk", tableNamePrefix), entity, m, Keys{
			PartitionKeys:     partitionKeyFields,
			ClusteringColumns: clusteringKeyFields,
		}),
		clusteringKeyFields: clusteringKeyFields,
		partitionKeyFields:  partitionKeyFields,
	}
}

func (k *k) TimeSeriesTable(tableNamePrefix, timeField, clusteringKeyField string, bucketSize time.Duration, entity interface{}) TimeSeriesTable {
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	return &timeSeriesT{
		table: k.NewTable(fmt.Sprintf("%s_timeSeries_%s_%s_%s", tableNamePrefix, timeField, clusteringKeyField, bucketSize), entity, m, Keys{
			PartitionKeys:     []string{bucketFieldName},
			ClusteringColumns: []string{timeField, clusteringKeyField},
		}),
		timeField:          timeField,
		clusteringKeyField: clusteringKeyField,
		bucketSize:         bucketSize,
	}
}

func (k *k) MultiTimeSeriesTable(tableNamePrefix, partitionKeyField, timeField, clusteringKeyField string, bucketSize time.Duration, entity interface{}) MultiTimeSeriesTable {
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}
	m[bucketFieldName] = time.Now()
	return &multiTimeSeriesT{
		table: k.NewTable(fmt.Sprintf("%s_multiTimeSeries_%s_%s_%s_%s", tableNamePrefix, partitionKeyField, timeField, clusteringKeyField, bucketSize.String()), entity, m, Keys{
			PartitionKeys:     []string{partitionKeyField, bucketFieldName},
			ClusteringColumns: []string{timeField, clusteringKeyField},
		}),
		partitionKeyField:  partitionKeyField,
		timeField:          timeField,
		clusteringKeyField: clusteringKeyField,
		bucketSize:         bucketSize,
	}
}

func (k *k) MultiKeyTimeSeriesTable(tableNamePrefix string, partitionKeyFields []string, timeField string, clusteringKeyFields []string, bucketSize time.Duration, entity interface{}) MultiKeyTimeSeriesTable {
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}

	partitionKeys := partitionKeyFields
	partitionKeys = append(partitionKeys, bucketFieldName)
	clusteringColumns := []string{timeField}
	clusteringColumns = append(clusteringColumns, clusteringKeyFields...)

	m[bucketFieldName] = time.Now()
	return &multiKeyTimeSeriesT{
		table: k.NewTable(fmt.Sprintf("%s_multiKeyTimeSeries_%s_%s", tableNamePrefix, timeField, bucketSize.String()), entity, m, Keys{
			PartitionKeys:     partitionKeys,
			ClusteringColumns: clusteringColumns,
		}),
		partitionKeyFields:  partitionKeyFields,
		timeField:           timeField,
		clusteringKeyFields: clusteringKeyFields,
		bucketSize:          bucketSize,
	}
}

func (k *k) FlakeSeriesTable(tableNamePrefix, flakeIDField string, bucketSize time.Duration, entity interface{}) FlakeSeriesTable {
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}
	m[flakeTimestampFieldName] = time.Now()
	m[bucketFieldName] = time.Now()
	return &flakeSeriesT{
		table: k.NewTable(fmt.Sprintf("%s_flakeSeries_%s_%s", tableNamePrefix, flakeIDField, bucketSize.String()), entity, m, Keys{
			PartitionKeys:     []string{bucketFieldName},
			ClusteringColumns: []string{flakeTimestampFieldName, flakeIDField},
		}),
		clusteringKeyField: flakeIDField,
		bucketSize:         bucketSize,
	}
}

func (k *k) MultiFlakeSeriesTable(tableNamePrefix, partitionKeyField, flakeIDField string, bucketSize time.Duration, entity interface{}) MultiFlakeSeriesTable {
	m, ok := toMap(entity)
	if !ok {
		panic("Unrecognized row type")
	}
	m[flakeTimestampFieldName] = time.Now()
	m[bucketFieldName] = time.Now()
	return &multiFlakeSeriesT{
		table: k.NewTable(fmt.Sprintf("%s_multiflakeSeries_%s_%s_%s", tableNamePrefix, partitionKeyField, flakeIDField, bucketSize.String()), entity, m, Keys{
			PartitionKeys:     []string{partitionKeyField, bucketFieldName},
			ClusteringColumns: []string{flakeTimestampFieldName, flakeIDField},
		}),
		clusteringKeyField: flakeIDField,
		bucketSize:         bucketSize,
		partitionKeyField:  partitionKeyField,
	}
}

type tableInfoMarshal struct {
	TableName string `cql:"table_name"`
}

// Returns table names in a keyspace
func (k *k) Tables() ([]string, error) {
	if k.qe == nil {
		return nil, fmt.Errorf("no query executor configured")
	}

	res := []tableInfoMarshal{}
	stmt := SelectStatement{
		keyspace: "system_schema",
		table:    "tables",
		fields:   []string{"table_name"},
		where:    []Relation{Eq("keyspace_name", k.name)},
	}
	err := k.qe.Query(stmt, NewScanner(stmt, &res))
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, v := range res {
		ret = append(ret, v.TableName)
	}
	return ret, nil
}

func (k *k) Exists(cf string) (bool, error) {
	ts, err := k.Tables()
	if err != nil {
		return false, err
	}
	for _, v := range ts {
		if strings.ToLower(v) == strings.ToLower(cf) {
			return true, nil
		}
	}
	return false, nil
}

func (k *k) DropTable(cf string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", k.name, cf)
	stmt := cqlStatement{query: query}
	return k.qe.Execute(stmt)
}

func (k *k) Name() string {
	return k.name
}
