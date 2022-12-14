import os
import cx_Oracle
import time
from pandas import read_sql_query
from pandas import DataFrame
from functools import reduce
from datetime import datetime


def strfdb(val):
    try:
        return val.strftime('%Y-%m-%d')
    except:
        return '' if val == None else val

class EMSDB:
    def __init__(self, tnsAdmin=r'I:\appl\TechOraClients\tns_admin\aso'):
        os.environ['TNS_ADMIN'] = tnsAdmin
        if 'ORACLE_HOME' in os.environ:
            del os.environ['ORACLE_HOME'] 
        if 'ORACLE_HOME_NAME' in os.environ:
            del os.environ['ORACLE_HOME_NAME'] 

    def try_get_retry(self, num: int, fn, default=None):
        for i in range(num):
            while True:
                try:
                    return fn()
                except cx_Oracle.DatabaseError as err:
                    print('DB error, trying again in {0} ({1} of {2})'.format(1 * 10 ** i, i + 1, num))
                    time.sleep(1 * 10 ** i)
                    break
        print('DB error, giving up!')
        if default is None:
            raise err
        return default
    
    def execute(self, database, execute: str, params:dict = None, default: DataFrame = DataFrame()) -> DataFrame:
        with self.try_get_retry(3, lambda: cx_Oracle.connect('', '', database)) as connection:
            cur = connection.cursor()
            cur.execute(execute)
            connection.commit()

    def query(self, database, query: str, params:dict = None, default: DataFrame = DataFrame()) -> DataFrame:
        with self.try_get_retry(3, lambda: cx_Oracle.connect('', '', database)) as connection:
            return read_sql_query(query, con=connection, params=params)
