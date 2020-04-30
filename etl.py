#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Load data into the tables


@author: udacity, ucaiado

Created on 04/25/2020
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Load data into staging tables

    :param cur. psycopg2 object.
    :param conn. psycopg2 object.
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Load data into analytical tables

    :param cur. psycopg2 object.
    :param conn. psycopg2 object.
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Load data into stanging and analytical tables
    '''
    config = configparser.ConfigParser()
    config.read('confs/dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()))
    cur = conn.cursor()

    print('...load staging tables')
    load_staging_tables(cur, conn)
    print('...insert data')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
