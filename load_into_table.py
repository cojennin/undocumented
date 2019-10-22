import pandas as pd
from pandas.io import sql
from pandas.io.sql import SQLTable
import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Boolean
import os
import csv

def load_into_sql(path):
    engine = sqlalchemy.create_engine('postgres+pg8000://i703642@127.0.01/documented')
    connection = engine.connect()

    ## We need to find the schema file first
    table_column_type = {}
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            if "EOIRDB_Schema.csv" in name:
                csv_to_read = f'{root}/{name}'
                with open(csv_to_read, 'r') as csvfile:
                    data = csv.reader(csvfile, delimiter='\t')
                    next(data)
                    for row in data:
                        if row[0] in table_column_type:
                            table_column_type[row[0]][row[1]] = row[2]
                        else: 
                            table_column_type[row[0]] = {
                                row[1]: row[2]
                            }

    for root, dirs, files in os.walk(path, topdown=False):
         for name in filter(lambda x: "EOIRDB_Schema.csv" != x , files):
            if ".csv" in name:
                csv_to_read = f'{root}/{name}'
                print(f'Reading csv: {csv_to_read}')
                conn = engine.connect()
                trans = None
                
                table_name = name[:-4]
                metadata = MetaData()
                metadata.reflect(bind=engine)

                def column_creator(name, typ):
                    column_type = String
                    if typ is "int":
                        column_type = Integer
                    else if typ is "bit"
                        column_type = Boolean
                    
                    Column(name, column_type)


                table_cols = table_column_type[table_name].keys().
                
                table = Table(
                    table_name, 
                    meta, 
                    ,
                )
                # table = metadata.tables[table_name]

                with open(csv_to_read, 'r') as csvfile:
                    print(f'Writing csv: {table_name}')
                    data = csv.reader(csvfile, delimiter='\t')
                    fields = next(data)
                    index = 0
                    for row in data:
                        items = dict(zip(fields, row))
                        for key in items:
                            if table_column_type[table_name][key] is 'int':
                                items[key] = int(items[key])


                        if index % 10000 is 0:
                            if index is 0:
                                trans = conn.begin()
                            else:
                                trans.commit()
                                trans = conn.begin()
                        connection.execute(table.insert(), **items)
                    trans.commit()
                # try:
                #     df = pd.read_csv(csv_to_read, sep='\t', encoding='utf-8')
                # except:
                #     df = pd.read_csv(csv_to_read, sep='\t+', encoding='utf-8')

                # table_name = name[:-4]
                # metadata = MetaData()
                # metadata.reflect(bind=engine)
                # table = metadata.tables[table_name]

                # print(f'Writing csv: {table_name}')
                # conn = engine.connect()
                # trans = None
                # for index, row in df.iterrows():
                #     if index % 10000 is 0:
                #         if index is 0:
                #             trans = conn.begin()
                #         else:
                #             trans.commit()
                #             trans = conn.begin()
                    
                #     connection.execute(table.insert(), **row)
                # trans.commit()
                    # table.insert().values(row)

                # a = a.replace(to_replace=r'.', value='', regex=True)
                # a.to_sql(con=engine, name=table_name, if_exists='replace', chunksize=5000)

load_into_sql("files")
