"""Database connection utilities"""


from IPython import get_ipython

# Monkey-patch sql insertion to enable multiple-row inserts 
# taken from https://github.com/pandas-dev/pandas/issues/8953

from pandas.io.sql import SQLTable

def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict((k, v) for k, v in zip(keys, row)) for row in data_iter]
    conn.execute(self.insert_statement().values(data))


def monkey_patch_pandas_sql():
    """Older versions of pandas performed inserts very slowly, so this was a fix.
    If you're using current versions of pandas I don't think this is necessary"""
    SQLTable._execute_insert = _execute_insert


import sys

if sys.version_info.major == 3:
    import configparser
else:
    import ConfigParser as configparser
import os

import pandas as pd
import pandas.io.sql as psql
import psycopg2

from sqlalchemy import create_engine


SECTION = 'database_creds'


def fetchDBCredentials(dbcred_file, section=SECTION):
    """
       Read database access credentials from dbcred_file and return 
       dict with host, database, user, password, port.
    """
    #Read database credentials from user supplied file
    conf = configparser.ConfigParser()
    conf.read(dbcred_file)
    return dict(conf.items(section))


def connect(dbcred_file, section=SECTION):
    try:
        conf = fetchDBCredentials(dbcred_file, section=section)
    except:
        try:
            conf = fetchDBCredentials('../' + cred_file)
        except:
            print("""Can't find credentials""")
            raise
    db_engine = create_engine(
        'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
            **fetchDBCredentials(dbcred_file, section=section)
        )
    ).execution_options(autocommit=True)
    #conn = psycopg2.connect(**conf)
    #conn.autocommit = True
    return db_engine


def register_sql_magic():
    ipython = get_ipython()
    ipython.magic("load_ext sql_magic")
    

def assign_conn_sql_magic(conn, conn_name='conn'):
    ipython = get_ipython()
    ipython.user_global_ns[conn_name] = conn
    ipython.magic("config SQL.conn_name = 'conn'")
    return


def connect_and_register_sql_magic(dbcred_file, section=SECTION, conn_name='conn'):
    """
    dbcred_file:  path to config file specifying database connection details
    section:  section name within the config file containing the connection detailas
    conn_name:  name in global namespace that the connection object will be assigned to"""

    connection = connect(dbcred_file, section)
    register_sql_magic()
    assign_conn_sql_magic(connection, conn_name)
    print("Connection object assigned to `{}`".format(conn_name))
    return
