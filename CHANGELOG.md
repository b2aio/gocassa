# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## v2.0.2 - 2019-07-01

### Fixed
 - If an error is thrown whilst scanning/reading a single row, the original pointer will stay untouched (previously it may have been allocated with the initial value of your struct)

## v2.0.1 - 2019-06-28

### Changed
 - Scannable has been expanded to take in a gocql Scannable. This is so the error handling can be greatly improved

### Fixed
 - Fixed issue masking all gocql iterator errors as RowNotFound when reading a single row

## v2.0.0 - 2019-06-22

*Note*: This is a major version change and quite a lot of the internals have changed to make decoding substantially faster. You will need to make some tweaks to your code if you are upgrading from v1. Please see the updated `interfaces.go` and the example in `gocql_backend.go` for updated usage.

### Changed
 - QueryExecutor now returns statements as a Statement interface object rather than a raw string
 - MapStructure has been completely removed as a dependency. Gocassa now does custom reflection coordinated with gocql
 - The use of the QueryExecutor has changed completely. See `gocql_backend.go` for an updated example.
 - Public interfaces now use Statement objects rather than just plain strings and values for queries
 - Using the scanner will also encapsulate decoding the objects into your structs (previously this happened after the QueryExeuctor returned on `Query` and `QueryWithOptions` function calls)

## v1.2.0 - 2015-12-22

### Added
 - Implemented `ORDER BY` for read queries

### Changed
 - Updated mock tables to use new method of decoding, now also supports embedded and non-key map types.

## v1.1.0 - 2015-11-27

### Fixed
 - Fixed incorrect ordering of results in `MockTable`
 - Fixed issue causing `Set` to fail with "PRIMARY KEY part user found in SET part" if keys are lower-case.

## v1.0.0 - 2015-11-13

### Added
 - Allow creating tables with compound keys
 - Added the `MultimapMultiKeyTable` recipe which allows for CRUD operations on rows filtered by equality of multiple fields.
 - Add support for compact storage and compression
 - Add `CreateIfNotExistStatement` and `CreateIfNotExist` functions to `Table`

### Changed
 - Improved how gocassa handles encoding+decoding, it no longer uses the `encoding/json` package and now supports embedded types and type aliases.
 - Added new functions to `QueryExecutor` interface (`QueryWithOptions` and `ExecuteWithOptions`)

### Fixed
 - Mock tables are now safe for concurrent use
 - `uint` types are now supported, when generating tables the cassandra `varint` type is used.
 - Fixed gocassa using `json` tags when decoding results into structs
