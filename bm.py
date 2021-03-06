#!/usr/bin/python3

import argparse
import datetime
import json
import psycopg2
from psycopg2 import sql
import re
from statistics import mean, median

TableNameDefault='roads_rdr'
TimesDefault=10

ExecTimeMsRe = re.compile('execution\s+time\s*:\s*(\d+(\.\d+)?)', re.IGNORECASE)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, help='Config')
    parser.add_argument('--data', required=True, help='Data file')
    parser.add_argument('--table', default=TableNameDefault, help="Table name")
    parser.add_argument('--times', type=int, default=TimesDefault, help='# of times to run CREATE INDEX/SELECT queries')
    parser.add_argument('--verbose', action='store_true', default=False, help='Print log messages')
    return parser.parse_args()

def check_config(config):
    if not 'connections' in config:
        raise Exception("`connections' field missing from config")
    elif not isinstance(config['connections'], list):
        raise Exception("`connections' field is not an array")

def load_config(path):
    with open(path, 'r') as fd:
        config = json.load(fd)
        check_config(config)
        return config

def connect(params):
    connection = psycopg2.connect(**params)
    connection.autocommit = True
    return connection

def run_script(cursor, path):
    with open(path, 'r') as fd:
        cursor.execute(fd.read())

def create_table(cursor, table):
    table_ident = sql.Identifier(table)
    query = 'CREATE TABLE {} (objectid integer, geom geometry(MultiLineStringZ, 3005))'
    cursor.execute(sql.SQL(query).format(table_ident))

def drop_table(cursor, table):
    table_ident = sql.Identifier(table)
    cursor.execute(sql.SQL('DROP TABLE IF EXISTS {}').format(table_ident))

def drop_index(cursor, name):
    index_ident = sql.Identifier(name)
    query = 'DROP INDEX IF EXISTS {}'
    cursor.execute(sql.SQL(query).format(index_ident))

def get_exec_time(answer):
    for line in answer:
        match = ExecTimeMsRe.match(line[0])
        if match:
            return float(match[1])
    raise Exception('Failed to get SELECT query execution time')

def test_create_index(cursor, table, index):
    table_ident = sql.Identifier(table)
    index_ident = sql.Identifier(index)
    now = datetime.datetime.now()
    query = 'CREATE INDEX {} ON {} USING GIST(geom)'
    cursor.execute(sql.SQL(query).format(index_ident, table_ident))
    time_ms = (datetime.datetime.now() - now).total_seconds() * 1000
    return time_ms

def test_query(cursor, table):
    table_ident = sql.Identifier(table)
    query = 'EXPLAIN ANALYZE SELECT COUNT(*) FROM {} a, {} b WHERE a.geom && b.geom'
    cursor.execute(sql.SQL(query).format(table_ident, table_ident))
    return get_exec_time(cursor.fetchall())

def test_index_size(cursor, name):
    query = 'SELECT pg_relation_size(%s)'
    cursor.execute(sql.SQL(query), (name,))
    return cursor.fetchone()[0]

def run(connection_params, table, data_path, times, verbose):
    # Connect
    if verbose: print('Connecting')
    connection = connect(connection_params)
    cursor = connection.cursor()
    # Drop and re-create data table
    if verbose: print('Dropping data table')
    drop_table(cursor, table)
    #create_table(cursor, table)
    # Import data
    if verbose: print('Importing data')
    run_script(cursor, data_path)
    # Close connection
    connection.close()
    if verbose: print('Connection closed')

    # Re-connect
    if verbose: print('Re-connecting')
    connection = connect(connection_params)
    cursor = connection.cursor()
    index = '{}_idx'.format(table)

    # Run tests
    if verbose: print('Running CREATE INDEX/SELECT {} time(s)'.format(times))
    create_index_time_ms = []
    select_time_ms = []
    for i in range(1, times + 1):
        drop_index(cursor, index)
        # Create index
        if verbose: print('  #{} CREATE INDEX: '.format(i), end='')
        time_ms = test_create_index(cursor, table, index)
        create_index_time_ms.append(time_ms)
        if verbose: print('{} ms'.format(time_ms))
        # Select
        if verbose: print('  #{}: SELECT: '.format(i), end='')
        time_ms = test_query(cursor, table)
        select_time_ms.append(time_ms)
        if verbose: print('{} ms'.format(time_ms))
    # Index size
    if verbose: print('Getting index size: ', end='')
    index_size = test_index_size(cursor, table)
    if verbose: print(index_size)

    # cleanup
    if verbose: print('Dropping data table')
    drop_table(cursor, table)

    # Print results
    if verbose: print()
    print("CREATE INDEX time, avg: {} ms, median: {} ms".format(
        mean(create_index_time_ms),
        median(create_index_time_ms)))
    print("SELECT execution time, avg: {} ms, median: {} ms".format(
        mean(select_time_ms),
        median(select_time_ms)))
    print("Index size: {}".format(index_size))

def main():
    args = parse_args()

    try:
        config = load_config(args.config)
        for connection_params in config['connections']:
            run(connection_params, args.table, args.data, max(1, args.times), args.verbose)
            print()

    except BaseException as e:
        print(e)
        raise e


if __name__ == '__main__':
    main()
