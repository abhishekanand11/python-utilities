__author__ = 'abhishekanand11'

#!/usr/bin/env python
# -*- coding: utf-8 -*-

#----------- All the imports that can be used in the scope of this script. Many of these might not be used as of now--------------------
import arrow
import psycopg2 as redshiftdbapi
import logging
import os
import csv
import sys
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import pandas as pd
import pandasql as psql
import shutil
import MySQLdb as mysqldbapi



#----------------------------------- Logging Handles Declaration. Not being used at this point of time --------------------

logger = logging.getLogger()
logger.setLevel(logging.NOTSET)

#File Handler
fh = logging.FileHandler('logger.log')
fh.setLevel(logging.INFO)

#Stream Handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# Add formatter to handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt="%d-%m-%Y %H:%M:%S")
ch.setFormatter(formatter)
fh.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)



#-------------------------------- MySQL Hosts and Redshift Hosts declaration -------------------------------------------

REDSHIFT_HOST = 'rsc.abc.com'
REDSHIFT_PORT = 5439
REDSHIFT_USERNAME = 'redshiftusername'
REDSHIFT_PASSWORD = 'Thisisredshiftpassword'
REDSHIFT_DATABASE = 'redshiftdatabase'

MYSQL_HOST = 'db.abc.com'
MYSQL_PORT = 3306
MYSQL_DATABASE = 'mysqldatabase'
MYSQL_USERNAME = 'mysqluser'
MYSQL_PASSWORD = 'Thisismysqluser'

#------------- Used to set global date variables to be used in the programme at a later point ----------

def set_dates(start_date_object):
    global recon_date_obj
    global recon_end_date_obj
    global recon_date
    global start_time
    global end_time
    recon_date_obj = start_date_object
    recon_end_date_obj=recon_date_obj
    recon_date = recon_date_obj.format('YYYY-MM-DD')
    start_time = recon_date_obj.format('YYYY-MM-DD HH:mm:ss')
    end_time = recon_date_obj.ceil('day').format('YYYY-MM-DD HH:mm:ss')

#------------ This is used to read inputs from trigger(mainly dates here for a given job --------------------

#--- Default dates are set here

current_date_obj=arrow.utcnow().to('IST').floor('day')
recon_date_obj=arrow.utcnow().to('IST').floor('day').replace(days=-1)
recon_end_date_obj=recon_date_obj

recon_date = recon_date_obj.format('YYYY-MM-DD')
start_time = recon_date_obj.format('YYYY-MM-DD HH:mm:ss')
end_time = recon_date_obj.ceil('day').format('YYYY-MM-DD HH:mm:ss')

if len(sys.argv) == 2:
    set_dates(arrow.get(sys.argv[1]).to('IST').floor('day'))
elif len(sys.argv) == 3:
    set_dates(arrow.get(sys.argv[1]).to('IST').floor('day'))
    recon_end_date_obj=arrow.get(sys.argv[2]).to('IST').floor('day')
elif len(sys.argv) > 3:
    logger.error("Invalid number of args")
    sys.exit(1)


#------- Used to create directories ----------------
def create_directory(recon_date):
    if not os.path.exists(os.path.join(PYTHON_SCRIPT_OUTPUT_PATH, recon_date)):
        os.makedirs(os.path.join(PYTHON_SCRIPT_OUTPUT_PATH, recon_date))

#--------------------------------------------- Actual Business Logic ------------------------------
def main():
    logger.info("OVERALL DATE RANGE - " + recon_date_obj.format('YYYY-MM-DD') + "  " + recon_end_date_obj.format('YYYY-MM-DD'))
    if recon_date_obj == recon_end_date_obj:
        logger.info("Processing for " + recon_date_obj.format('YYYY-MM-DD HH:mm:ss'))
        process_for_recon_date(recon_date_obj)
    elif recon_date_obj < recon_end_date_obj:
        DATE_RANGE = get_date_range(recon_date_obj, recon_end_date_obj)
        for date in DATE_RANGE:
            logger.info("Processing for " + date.format('YYYY-MM-DD HH:mm:ss'))
            process_for_recon_date(date)
    else:
        logger.error("Invalid start date and end date")
        sys.exit(1)
        

def process_for_recon_date(recon_process_date):
    set_recon_dates(recon_process_date)





query = """ Select columnName from table where condition """

#----- This is used to build MySQL connection ---------------
mysql_connection = dbapi.connect(host=MYSQL_HOST, user=MYSQL_USERNAME, passwd=MYSQL_PASSWORD, db=MYSQL_DATABASE, connect_timeout=60)
data = pandas.io.sql.read_sql(query, mysql_connection)
columnNames = data.columnName.values.tolist()


#------ This is used to build Redshift Connection -----------
conn = psycopg2.connect(host=REDSHIFT_HOST, port=REDSHIFT_PORT, database=REDSHIFT_DATABASE, user=REDSHIFT_USERNAME, password=REDSHIFT_PASSWORD)
all_ag_data = pd.io.sql.read_sql(query, conn)





#------------- Entry point of programme --------------
if __name__ == '__main__':
    main()


