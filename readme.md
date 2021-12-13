# Requirements
Python 3, psycopg2

# Usage
For each connection, the script creates `roads_rdr` table with some geospatial data,
then runs:
- `CREATE INDEX roads_rdr_idx ON roads_rdr USING GIST(geom)`
- `EXPLAIN ANALYZE SELECT COUNT(*) FROM roads_rdr a, roads_rdr b WHERE a.geom && b.geom`
- `SELECT pg_relation_size(roads_rdr_idx)`

$ python3 /path/to/bm.py --config config.json --data roads_rdr_insert.sql

# NOTES
- `roads_rdr` table will be deleted if exists.
- Index creation time includes network latency, as there's no way to directly measure it.

# TODO
- Print connection names

