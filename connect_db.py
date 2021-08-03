import sqlalchemy as sal
from decouple import config
from sqlalchemy import create_engine
import pandas as pd


user = config('user')
# print(user)
password = config('password')
host = config('host')
port = config('port')
db = config('db')

engine = sal.create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(user,password,host,port,db))
conn = engine.connect()

q = engine.execute('''select cp.id, cp.name, cw.url from company_profile as cp 
                    join company_website as cw on cp.id = cw.company_profile_id
                    order by cp.id ;''')
query_result = q.fetchall()

with open('company_id_and_websites_test.txt','w',encoding='utf8') as wf:
    for row in query_result:
        wf.write('\t'.join([str(x) for x in list(row)])+'\n')
