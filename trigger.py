#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ......
# ......

import os
import re
import time
import pprint
import math
import datetime
import subprocess , sys
from datetime import date
from datetime import timedelta
import pipes  # for shell-quoting, pipes.quote()

#try:
import psycopg2
from conn.config import config
#except ImportError , e:
#   sys.exit('Cannot import modules.  Please check that you have psycopy2,postgreSQL-devel Package  Detail: ' + str(e))

LOG_FILE='trigger.log'
CURR_DATE=datetime.datetime.now().strftime("%Y%m%d")

# SQL STATEMENT 

GET_DB_VERSION = """SELECT version()"""

#GET_ALL_DATA_TABLES_SQL = """
#select n.nspname as schemaname, c.relname as tablename from pg_class c, pg_namespace n where
#c.relnamespace = n.oid and c.relkind='r'::char and (c.relnamespace > 16384 or n.nspname = 'public' or n.nspname = 'pg_catalog') and c.oid not in (select reloid from pg_exttable)
#EXCEPT
#select distinct schemaname, tablename from (%s) AS pps1
#EXCEPT
#select distinct partitionschemaname, parentpartitiontablename from (%s) AS pps2 where parentpartitiontablename is not NULL
#""" % (PG_PARTITIONS_SURROGATE, PG_PARTITIONS_SURROGATE)


#How To Excute SQL Statment


# Create a Command object that executes a query using psql.
#def create_psql_command(dbname, query):
#    psql_cmd = """psql %s -c %s""" % (pipes.quote(dbname), pipes.quote(query))
#    return Command(query, psql_cmd)

def run_sql(query):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)    
        cur = conn.cursor()
        # execute a statement
        print('execute a statement:')
        cur.execute('SELECT version()')
        res = cur.fetchall()
#        print(res)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    cur.close()
    return res

#def generate_timestamp():
#    timestamp = datetime.now()
#    return timestamp.strftime("%Y%m%d%H%M%S")

#def construct_oid_regclass_str(schema_table):
#    schema = schema_table[0]
#    table = schema_table[1]

#    return "'" + pg.escape_string("%s.%s" % (escape_identifier(schema), escape_identifier(table))) + "'::regclass"

#def validate_schema_exists(pg_port, dbname, schema):
#    conn = None
#    try:
#        dburl = dbconn.DbURL(port=pg_port, dbname=dbname)
#        conn = dbconn.connect(dburl)
#        count = dbconn.execSQLForSingleton(conn, "select count(*) from pg_namespace where nspname='%s';" % pg.escape_string(schema))
#        if count == 0:
#            raise ExceptionNoStackTraceNeeded("Schema %s does not exist in database %s." % (schema, dbname))
#    finally:
##        if conn:
#            conn.close()


# Other Useful Function

def logMsg (msg):
    # define filename
    filename = LOG_FILE + '.' + time.strftime("%Y%m%d")
    ofh = open(filename, "a")
    ofh.write(time.strftime("[%Y/%m/%d-%H:%M:%S] "))
    ofh.write(msg)
    ofh.write("\n")
    ofh.close()

if __name__ == '__main__':
    logMsg("[SQLCMD]: "+GET_DB_VERSION)
    run_sql(GET_DB_VERSION)
