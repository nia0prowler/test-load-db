import os
import sys
import pyodbc
import pandas as pd
import sqlalchemy as sql
from subprocess import check_output, call


connString = 'mssql+pyodbc://admin_user:Ytrewq22@{}/{}?driver={}'
server = 'test-subscription.database.windows.net'
driver = 'ODBC+Driver+13+for+SQL+Server'
db = 'ml_fks_dev'
username='admin_user'
password='Ytrewq22'
delimiter='|'
login_string = '-U {} -P "{}"'.format(username, password)
tablename = 'ModelII'
schema = 'dbo'


#remove the delimiter and change the encoding of the data frame to latin so sql server can read it
#df.loc[:,df.dtypes == object] = df.loc[:,df.dtypes == object].apply(lambda col: col.str.replace(delimiter,''))
#df.to_csv(temp_file, index = False, sep = delimiter, encoding='utf-8')

o = call('bcp {}.{} in "{}" -b 10000 -S "{}" -d {} {} -q -c -t "{}"'.format(
    schema, tablename, temp_file, server, db, login_string, delimiter), shell=True)


print('[{}].[{}].[{}] filled'.format(db, schema, tablename))

