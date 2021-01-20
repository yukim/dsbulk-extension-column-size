# DSBulk workflow for column size statistics

The extension workflow for [dsbulk](https://github.com/datstax/dsbulk) that works similar to `unload` workflow, but instead of writing out records, this workflow writes the size of columns.

Primary usage for this is to find out if the table has large columns and the size of the column is above [DataStax Astra's column size limitation (5MB)](https://docs.astra.datastax.com/docs/datastax-astra-database-limits).

Since some column types are fixed size, only the column of the following types are writen.

- ascii/text/varchar
- blob
- custom
- list
- map
- set
- udt
- tuple

## Usage

```shell
dsbulk column-size -k <keyspace_name> -t <table_name>
```

By default, the above command will output the primary key columns with actual values, and regular columns with the size in bytes.

The output will be omitted for the records containing the column that the size of column is below 5MB by default.

You can change the threshold by specifying `--dsbulk.columnSize.columnSizeThreshold` in bytes. For example, if you want to output all the rows that have column size of 100KB and above, do:

```shell
dsbulk column-size -k <keyspace_name> -t <table_name> --dsbulk.columnSize.columnSizeThreshold=102400
```

You can use all the options that `unload` supports.

If you want to output the result in a file, use `-url` option.

## Example

If you have the following table:

```sql
CREATE TABLE test (
  id text,
  ts timeuuid,
  value blob,
  tags set<text>,
  PRIMARY KEY ((id), ts)
);
```

then, the output of the following command will be:

```shell
> dsbulk column-size -k ks -t test --dsbulk.columnSize.columnSizeThreshold=0
Operation directory: /path/to/dsbulk-extension-column-size/logs/COLUMN_SIZE_20210120-033230-246000
Column size threshold: 0 bytes
id,ts,size_of__tags,size_of__value
002,d51cb2d1-5acf-11eb-b70f-9116fc548b6b,27,123
001,bf467d11-5acf-11eb-b70f-9116fc548b6b,18,27
001,c4a43cc0-5acf-11eb-b70f-9116fc548b6b,11,14
total | failed | rows/s | p50ms | p99ms | p999ms
    3 |      0 |      5 |  6.24 |  6.59 |   6.59
Operation COLUMN_SIZE_20210120-033230-246000 completed successfully in less than one second.
=== Column size report ===
Total rows: 3
Above threshold: 3
Percentage: 100.0%

--- size_of__value ---
Count: 3
Min: 14 bytes
Mean: 54.666666666666664 bytes
Max: 123 bytes
95%tile: 123.0 bytes
99%tile: 123.0 bytes

--- size_of__tags ---
Count: 3
Min: 11 bytes
Mean: 18.666666666666668 bytes
Max: 27 bytes
95%tile: 27.0 bytes
99%tile: 27.0 bytes
```

