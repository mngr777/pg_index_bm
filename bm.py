#!/usr/bin/python3

import argparse
import datetime
import psycopg2
from psycopg2 import sql
import json

TableName='roads_rdr'

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, help='Config')
    parser.add_argument('--data', required=True, help='Data file')
    parser.add_argument('--table', default=TableName)
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
    index_ident = sql.Identifier(name);
    query = 'DROP INDEX IF EXISTS {}'
    cursor.execute(sql.SQL(query).format(index_ident))

def print_exec_time(label, answer):
    ExecTimeTextPrefix = 'Execution Time: '
    for line in answer:
        if line[0][0:len(ExecTimeTextPrefix)] == ExecTimeTextPrefix:
            print(label, line[0][len(ExecTimeTextPrefix):])

def test_create_index(cursor, table, index):
    table_ident = sql.Identifier(table)
    index_ident = sql.Identifier(index)
    now = datetime.datetime.now()
    query = 'CREATE INDEX {} ON {} USING GIST(geom)'
    cursor.execute(sql.SQL(query).format(index_ident, table_ident))
    time_ms = (datetime.datetime.now() - now).total_seconds() * 1000
    print('Index creation:', time_ms, 'ms')

def test_query(cursor, table):
    table_ident = sql.Identifier(table)
    query = 'EXPLAIN ANALYZE SELECT COUNT(*) FROM {} a, {} b WHERE a.geom && b.geom'
    cursor.execute(sql.SQL(query).format(table_ident, table_ident))
    print_exec_time('Query time:', cursor.fetchall())

def test_index_size(cursor, name):
    query = 'SELECT pg_relation_size(%s)'
    cursor.execute(sql.SQL(query), (name,))
    size = cursor.fetchone()[0]
    print('Index size:', size)

def run(connection_params, table, data_path):
    # connect
    connection = connect(connection_params)
    cursor = connection.cursor()

    # drop and re-create data table
    drop_table(cursor, table)
    #create_table(cursor, table)
    # import data
    run_script(cursor, data_path)

    connection.close()

    connection = connect(connection_params)
    cursor = connection.cursor()

    index = '{}_idx'.format(table)
    drop_index(cursor, index)
    test_create_index(cursor, table, index)
    test_query(cursor, table)
    test_index_size(cursor, table)

    # cleanup
    drop_table(cursor, table)

def main():
    args = parse_args()

    try:
        config = load_config(args.config)
        for connection_params in config['connections']:
            run(connection_params, args.table, args.data)
            print()

    except BaseException as e:
        print(e)
        raise e


if __name__ == '__main__':
    main()
