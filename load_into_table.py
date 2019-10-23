import os
import csv
import codecs
import pandas as pd
import time
import sys
import shutil
import re
from dateutil.parser import parse
from pandas.io import sql
from pandas.io.sql import SQLTable
import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import BigInteger
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import Boolean
from sqlalchemy import DateTime
from prompt_toolkit import prompt

date_pattern = re.compile(r"\d{4}")

def get_files_at(path, filter_where):
    found = []
    for root, dirs, files in os.walk(path):
        for name in files:
            found.append(f'{root}/{name}')

    if filter_where:
        return filter(filter_where, found)
    else:
        return found

def find_file_at(path, filter_where):
    return next(get_files_at(path, filter_where))

def filtered_exists():
    return os.path.exists(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'files_filtered'))

def seek_status_exists():
    return os.path.isfile(os.path.join(os.path.realpath(__file__), '.seek_status'))

def create_table(table_name, table_column_type, engine):
    metadata = MetaData(engine)

    def column_creator(col):
        (name, col_type) = col

        maybe_column_type = String
        # if col_type == "int":
        #     maybe_column_type = BigInteger
        # elif col_type == "bit":
        #     maybe_column_type = SmallInteger
        # elif col_type == "datetime":
        #     maybe_column_type = DateTime
        
        return Column(name, maybe_column_type)

    table_cols = list(map(column_creator, zip(table_column_type[table_name].keys(), table_column_type[table_name].values())))
    
    table = Table(
        table_name, 
        metadata, 
        *table_cols
    )

    if not engine.dialect.has_table(engine, table_name):
        table.create()

    return table

def filter_field_data(field, table_name, table_column_type):
    (col_name, val) = field
    col_type = table_column_type[table_name][col_name]
    result = val.rstrip()
    if result == '':
        return (col_name, '')
    elif result == 'NULL':
        return (col_name, '')
    elif col_type == "int" and not result.replace('-', '').replace('+', '').isdigit():
        raise Exception("Invalid int type")
    elif col_type == "bit" and (result != "1" and result != '0'):
        raise Exception("Invalid bit type")
    elif col_type == "datetime" and date_pattern.match(result) is None:
        raise Exception("Invalid date time")
    else:
        return (col_name, result)

def load_into_sql(path):
    engine = sqlalchemy.create_engine('postgres+pg8000://i703642@127.0.01/documented')
    connection = engine.connect()

    # shutil.rmtree(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'files_filtered'))
    # if filtered_exists():
    #     remove_filtered_answer = prompt('filtered directory exists, remove and start from scratch? (N/y):')
    #     if remove_filtered_answer.lower() == 'y':
    #         shutil.rmtree(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'files_filtered'))

    # if seek_status_exists():
    #     remove_seek_answer = prompt('you stopped the program while it was uploading, pick up from where it left off? (Y/n):')
    #     if remove_seek_answer.lower() == 'n':
    #         os.remove(os.path.join(os.path.realpath(__file__), '.seek_status'))

    # We need to find the schema file and then map
    # tables to columns to column types
    table_column_type = {}
    schema_file = find_file_at(path, lambda x: "EOIRDB_Schema.csv" in x)
    with open(schema_file, 'r') as csvfile:
        data = csv.reader(csvfile, delimiter='\t')
        next(data)
        for row in data:
            if row[0] in table_column_type:
                table_column_type[row[0]][row[1]] = row[2]
            else: 
                table_column_type[row[0]] = {
                    row[1]: row[2]
                }

    files_to_filter = ["B_TblProceedCharges.csv"]

    csv_files = list(filter(lambda x: os.path.basename(x) in files_to_filter, list(get_files_at(path, lambda x: "EOIRDB_Schema.csv" not in x and ".csv" in x))))

    # Create the filtered files
    filtered_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'files_filtered')
    if filtered_exists() is not True:
        os.mkdir(filtered_path)
    
    with open(os.path.join(filtered_path, f'invalid.txt'), 'w') as invalid_file:
        invalid_writer = csv.writer(invalid_file)
        for csv_to_read in csv_files:
            name = os.path.basename(csv_to_read)[:-4]
            file_to_use = os.path.isfile(os.path.join(filtered_path, f'{name}.csv'))
            if os.path.isfile(file_to_use):
                print(f'{file_to_use} exists, delete it first')
            else:
                with open(os.path.join(filtered_path, f'{name}.csv'), 'w') as write_csv_file:
                    with codecs.open(csv_to_read, 'r', 'utf-8') as read_csv_file:
                        print(f'Creating filtered csv: {name}')
                        reader = csv.reader((line.replace('\0', ' ') for line in read_csv_file), delimiter='\t')
                        writer = csv.writer(write_csv_file)
                        fields = next(reader)
                        writer.writerow(fields)
                        index = 0
                        all_keys_for_table = dict.fromkeys(table_column_type[name].keys())
                        for row in reader:
                            try:
                                # items = dict(zip(fields, row))
                                # items = dict(list(map(lambda x: filter_field_data(x, name, table_column_type), zip(fields, row))))
                                cols = {
                                    **(all_keys_for_table),
                                    **(dict(zip(fields, row))),
                                }
                                
                                writer.writerow(cols.values())

                            except Exception as e:
                                msg = f'Exception with table {name}, row {index}, {str(e)}'
                                print(msg)
                                invalid_writer.writerow([msg])
                            index += 1

    # csv_files_to_load = list(get_files_at(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'files_filtered'), lambda x: os.path.basename(x) in files_to_filter))

    for csv_to_load in csv_files:
        trans = None         
        name = os.path.basename(csv_to_load)[:-4]

        table = create_table(name, table_column_type, engine)

        # Filter out any incorrect values
        # Ex: a date field has the value 'NOT A DATE'
        # This should raise an exception
        # 'NULL' we just assume should be null in the DB (so it's set to None)
        # Return a tuple (column name and the value)

        # with codecs.open(csv_to_load, 'r', 'utf-8') as csvfile:
        #     print(f'Writing csv: {name}')
        #     data = csv.reader(csv_to_load, delimiter='\t')
        #     fields = next(data)
        #     index = 0
        #     start = 0
        #     end = 0
        #     for row in data:
        #         ## Filter out NULL byte characters
        #         # filtered_row = list(map(filter_field_data, row))
        #         if index % 20000 is 0:
        #             if index is 0:
        #                 start = time.time()
        #                 trans = connection.begin()
        #             else:
        #                 trans.commit()
        #                 trans = connection.begin()
        #                 end = time.time()
        #                 print(f"Time (in seconds) to commit index #{index}, {end - start}")
        #         # `filter_field_data` will return a tuple of the column name and the filtered
        #         # field value OR it will raise an exception
        #         # Catch the exceptions and log, but don't do anythign with them yet
        #         items = dict(zip(fields, row))

        #         index += 1
        #         connection.execute(table.insert(), **items)
        #     trans.commit()

load_into_sql("files")