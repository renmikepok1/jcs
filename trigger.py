#!/usr/bin/env python
# -*- coding: utf-8 -*-
# import mainUtils FIRST to get python version check
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
import psycopg2
from conn.config import config

## CONFIG
LOOPTIME=1000
ONDEMAND_STR='10.11.50.96:9876'
LOG_FILE='trigger.log'
CURR_DATE=datetime.datetime.now().strftime("%Y%m%d")

## SQL_LIST  
GEN_JCS_STG_PROC_VIEW = """
create or replace view jcs_stg_proc as  
select tabname||'_'||to_char(end_execute_time, 'YYYYMMDD_HHMMSS') as filename,
REPLACE (tabname, 'STG', 'ODS') as odstabname,tabname,
source_file_ct,target_file_ct,start_execute_time,end_execute_time,
start_data_time,end_data_time,bu from jcs_stg where STATUS!='s'; """

DROP_JCS_STG_PROC_VIEW = """
drop view jcs_stg_proc;
"""

GET_JCS_STG_CT = """
SELECT COUNT(*) as cnt FROM jcs_stg_proc;"""

GET_JCS_STG_LIST = """
SELECT * FROM (
SELECT ods.job_id,stg.tabname,stg.filename,stg.bu,
stg.start_data_time,stg.end_data_time,ods.job_last_exec_dttm,ods.job_next_exec_dttm  FROM jcs_stg_proc stg, job_ctl_schedule_info ods
WHERE ods.job_name=stg.odstabname
) as aa WHERE to_char(job_last_exec_dttm, 'YYYYMMDD') = to_char(start_data_time, 'YYYYMMDD')
  AND to_char(job_next_exec_dttm, 'YYYYMMDD') = to_char(end_data_time, 'YYYYMMDD');
"""

## FUNCTION  
def run_sql(query):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        #print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)    
        cur = conn.cursor()
        # execute a statement
        cur.execute(query)
        #res1 = cur.fetchone()
        res = cur.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    cur.close()
    return res

def execute_sql(query):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        #print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)    
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    cur.close()

def generate_timestamp():
    timestamp = datetime.now()
    return timestamp.strftime("%Y%m%d%H%M%S")

def logMsg (msg):
    # define filename
    filename = LOG_FILE + '.' + time.strftime("%Y%m%d")
    ofh = open(filename, "a")
    ofh.write(time.strftime("[%Y/%m/%d-%H:%M:%S] "))
    ofh.write(msg)
    ofh.write("\n")
    ofh.close()

## Main Function
def proc_trigger():
    # COUNT HFILE TABLE
    res = run_sql(GET_JCS_STG_CT)
    for row in res:
        cnt=row[0]
    if str(cnt) == '0':
        logMsg(" [INFO]: Skip Process Trigger & HTable count='"+str(cnt)+"'")
        print("Skip Process Trigger & HTable count='"+str(cnt)+"' ")
        return
    logMsg(" [INFO]: Process Trigger & HTable count='"+str(cnt)+"' ")
    print ("Process Trigger & HTable count='"+str(cnt)+"' ")
    
    #GET LIST FROM HFILE TABLE
    res = run_sql(GET_JCS_STG_LIST)
    for row in res:
        job_id = row[0]
        tabname = row[1]
        filename = row[2]
        bu = row[3]
        start_data_time = row[4]
        end_data_time = row[5]
        #print (job_id,tabname,filename,bu,start_data_time)

        ## Call On Demand...
        ExpSqlStmt ="curl -H \"Content-Type: application/json\""
        ExpSqlStmt+=" -d '{ \"job_id\" : \""+job_id+"\" , \"job_exec_dttm\" : \""+str(start_data_time)+"\" ,\"customized\" : ["
        ExpSqlStmt+=" { \"key\" : \"END.DT\" , \"value\" : \""+str(end_data_time)+"\" , \"desc\" : \"test no 1\" , \"enable\" : \"Y\"},"
        ExpSqlStmt+=" { \"key\" : \"filename\" , \"value\" : \""+filename+"\" , \"desc\" : \"test no 1\" , \"enable\" : \"Y\"} ]}'"
        ExpSqlStmt+=" http://"+ONDEMAND_STR+"/onDemandJob/" 
        print (ExpSqlStmt)
        #ret = os.popen(""+ExpSqlStmt+'" 2>&1');
        #logMsg("[RETURN]: "+ret.read()); 
        
        #UPDATE JCS_STG STATUS
        ExpSqlStmt =" UPDATE JCS_STG SET STATUS='s' WHERE TABNAME='" +tabname+"'"
        ExpSqlStmt+=" AND START_DATA_TIME='" + str(start_data_time) +"';"
        execute_sql(ExpSqlStmt)

if __name__ == '__main__':
    #logMsg("[SQLCMD]: "+GET_DB_VERSION)
    execute_sql(DROP_JCS_STG_PROC_VIEW)
    execute_sql(GEN_JCS_STG_PROC_VIEW)
    while True:
        try:
            proc_trigger()
            time.sleep(LOOPTIME)
        except KeyboardInterrupt:
            print('Manual break by user')
            break