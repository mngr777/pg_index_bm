# Requirements
Python 3, psycopg2

# Usage
For each connection, the script creates `roads_rdr` table with some geospatial data,
then runs:
- `CREATE INDEX roads_rdr_idx ON roads_rdr USING GIST(geom)`
- `EXPLAIN ANALYZE SELECT COUNT(*) FROM roads_rdr a, roads_rdr b WHERE a.geom && b.geom`
- `SELECT pg_relation_size(roads_rdr_idx)`

`CREATE INDEX` and `SELECT` queries will run `--times` times, 10 is default.
Add `--verbose` for more detailed output.

```
$ python3 /path/to/bm.py --config config.json --data roads_rdr_insert.sql --verbose
Connecting
Dropping data table
Importing data
Connection closed
Re-connecting
Running CREATE INDEX/SELECT 10 time(s)
  #1 CREATE INDEX: 137.17499999999998 ms
  #1: SELECT: 8311.573 ms
  #2 CREATE INDEX: 119.31700000000001 ms
  #2: SELECT: 8437.699 ms
  #3 CREATE INDEX: 115.98 ms
  #3: SELECT: 8406.936 ms
  #4 CREATE INDEX: 117.08300000000001 ms
  #4: SELECT: 8286.799 ms
  #5 CREATE INDEX: 119.896 ms
  #5: SELECT: 8363.925 ms
  #6 CREATE INDEX: 117.699 ms
  #6: SELECT: 8459.941 ms
  #7 CREATE INDEX: 117.383 ms
  #7: SELECT: 8711.058 ms
  #8 CREATE INDEX: 118.145 ms
  #8: SELECT: 8454.821 ms
  #9 CREATE INDEX: 116.944 ms
  #9: SELECT: 8382.684 ms
  #10 CREATE INDEX: 116.22200000000001 ms
  #10: SELECT: 8378.065 ms
Getting index size: 13582336
Dropping data table

CREATE INDEX time, avg: 119.5844 ms, median: 117.541 ms
SELECT execution time, avg: 8419.3501 ms, median: 8394.81 ms
Index size: 13582336
```

# NOTES
- `roads_rdr` table will be deleted if exists.
- Index creation time includes network latency, as there's no way to directly measure it.

# TODO
- Print connection names

