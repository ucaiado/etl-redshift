#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Load data into the tables


@author: udacity, ucaiado

Created on 04/25/2020
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drop all tables from sql_queries.py

    :param cur. psycopg2 object.
    :param conn. psycopg2 object.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Create all tables from sql_queries.py

    :param cur. psycopg2 object.
    :param conn. psycopg2 object.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Drop and create required tables in the Redshift cluster
    '''
    config = configparser.ConfigParser()
    config.read('confs/dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()))
    cur = conn.cursor()

    print('drop existing tables...')
    drop_tables(cur, conn)
    print('create new tables...')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
