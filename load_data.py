import os
import codecs
import csv
import sys
from pathlib import Path
import subprocess
from prompt_toolkit import prompt
import shutil
import zipfile
import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import BigInteger
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import Boolean
from sqlalchemy import DateTime
import urllib3
import pg8000
import configparser
import io
import re

http = urllib3.PoolManager()

EOIR_ZIP_URL = "https://fileshare.eoir.justice.gov/FOIA-TRAC-Report.zip"

EOIR_ZIP = "FOIA-TRAC-Report.zip"
EOIR_DATA = "eoir_data"
EOIR_DATA_PREPARED = "eoir_data_prepared"
SCHEMA_FILE = "schema.csv"
CONFIG_FILE = "config.ini"
PROGRAM_DIR = Path(os.path.dirname(os.path.realpath(__file__)))

config = configparser.ConfigParser()
config.read(PROGRAM_DIR / CONFIG_FILE)

DB_USER=config['DB']['DB_USER']
DB_HOST=config['DB']['DB_HOST']
DB_NAME=config['DB']['DB_NAME']
DB_PASSWORD=config['DB']['DB_PASSWORD']


def main():
  # remove_file(PROGRAM_DIR / EOIR_DATA_PREPARED / "tblAppealFedCourts.csv")
  # create_prepared_eoir_data_file(PROGRAM_DIR / EOIR_DATA / "tblAppealFedCourts.csv", PROGRAM_DIR / EOIR_DATA_PREPARED)
  # return

  ## If the zip file doesn't exist, should we download it? 
  if eoir_zip_exists():
    should_fetch_eoir_zip = is_prompt_yes_exact(prompt('EOIR zip file exists, delete and fetch it from the EOIR website? (N/y): '))
    if should_fetch_eoir_zip:
      print('Fetching EOIR zip file...')
      fetch_eoir_zip()
      print ('EOIR zip file downloaded, proceeding...')
  else:
    print('Fetching EOIR zip file...')
    fetch_eoir_zip()
    print ('EOIR zip file downloaded, proceeding...')
      
  ## If the unzipped data exists, should we overwrite it?
  ## Otherwise, proceed with unzipping
  if eoir_data_exists():
    should_overwrite_data = is_prompt_yes_exact(prompt('EOIR data already exists, overwrite it with new data zip file? (N/y) '))
    if should_overwrite_data:
      remove_eoir_data()
      unzip_file((PROGRAM_DIR / EOIR_ZIP), (PROGRAM_DIR / EOIR_DATA))
  else:
    unzip_file((PROGRAM_DIR / EOIR_ZIP), (PROGRAM_DIR / EOIR_DATA))
  
  # Prepare the CSV data for `COPY` into database
  # All rows have to have values for the header's for `COPY` command to work in POSTGRES
  if eoir_data_prepared_exists():
    should_overwrite_prepared_data = is_prompt_yes_exact(prompt('Prepared EOIR data already exists, overwrite it with new data? (N/y) '))
    if should_overwrite_prepared_data:
      remove_eoir_prepared_data()
      print('Preparing EOIR data, this can take a while...')
      create_prepared_eoir_data(PROGRAM_DIR / EOIR_DATA, PROGRAM_DIR / EOIR_DATA_PREPARED, lambda path: ".csv" in str(path) and "EOIRDB_Schema" not in str(path))
  else:
    print('Preparing EOIR data, this can take a while...')
    create_prepared_eoir_data(PROGRAM_DIR / EOIR_DATA, PROGRAM_DIR / EOIR_DATA_PREPARED, lambda path: ".csv" in str(path) and "EOIRDB_Schema" not in str(path))

  # should_upload_data = is_prompt_yes(prompt('Upload EOIR data to database? (Y/n) '))

  engine = sqlalchemy.create_engine(f'postgres+pg8000://{DB_USER}:{DB_PASSWRORD}@{DB_HOST}/{DB_NAME}')
  connection = engine.connect()

  should_upload_data = is_prompt_yes(prompt('Upload EOIR data to database? (Y/n) '))

  print("Maybe creating database tables...")

  if should_upload_data:
    create_tables_if_not_exists(engine, PROGRAM_DIR / EOIR_DATA_PREPARED, lambda path: ".csv" in str(path) and "EOIRDB_Schema" not in str(path))

  print("Tables created, uploading data...")

  upload_csv_files(PROGRAM_DIR / EOIR_DATA_PREPARED, lambda path: ".csv" in str(path) and "EOIRDB_Schema" not in str(path))

def is_prompt_yes_exact(answer):
  return answer.lower().strip() == 'y'

def is_prompt_yes(answer):
  return answer.lower().strip() == 'y' or answer.lower().strip() == ''

def is_prompt_no(answer):
  return answer.lower().strip() == 'n' or answer.lower().strip() == ''

def eoir_zip_exists():
  return (PROGRAM_DIR / EOIR_ZIP).exists()

def eoir_data_exists():
  return (PROGRAM_DIR / EOIR_DATA).exists()

def eoir_data_prepared_exists():
  return (PROGRAM_DIR / EOIR_DATA_PREPARED).exists()

def remove_eoir_data():
  shutil.rmtree(PROGRAM_DIR / EOIR_DATA, ignore_errors=True)

def remove_eoir_prepared_data():
  shutil.rmtree(PROGRAM_DIR / EOIR_DATA_PREPARED, ignore_errors=True)

def remove_file(file_path):
  file_path.unlink()

def unzip_file(path_to_zip, path_to_extract):
  #### SEE ISSUES WITH PYTHON zipfiled MODULE
  #### USING SUBPROCESS, RELYING ON USER TO HAVE UNZIP
  #### UNTIL THIS ISSUE IS FIXED
  #### THIS WILL CREATE COMPATABILITY ISSUES 
  subprocess.call(["unzip", "-j", str(path_to_zip), "-d", str(path_to_extract)])

  ### Getting a "NotImplemented" exception when trying to unzip certain files
  ### with Python's zipfile.
  ### "NotImplemented" Exception message: "compression type 9 (deflate64)"
  ### Happens for 
  ### - D_TblAssociatedBond.csv
  ### - B_TblProceeding.csv
  ### - tbl_schedule.csv
  ### Could be an issue with size of these csv's? Didn't seem like the compression type
  ### of the other csv's was type 9 (they were all type 8)
  ### Need to explore further

  # path_to_extract.mkdir(parents=True, exist_ok=True)
  # with zipfile.ZipFile(path_to_zip, 'r') as zip_ref:
  #   for filename in zip_ref.namelist(): 
  #     try:
  #       print(f"About to unzip {filename}")
  #       info = zip_ref.getinfo(filename)
  #       with zip_ref.open(filename) as zip_file:
  #         info = zip_ref.getinfo(filename)
  #         if not zip_ref.getinfo(filename).is_dir():
  #           with open(path_to_extract / os.path.basename(filename), 'wb') as writer:
  #             for byte in zip_file:
  #               writer.write(byte)
  #     except NotImplementedError as e:
  #       print(e)

def fetch_eoir_zip():
  r = http.request('GET', EOIR_ZIP_URL, preload_content=False)
  with open(PROGRAM_DIR / EOIR_ZIP, 'wb') as out:
    while True:
        data = r.read(4096)
        if not data:
            break
        out.write(data)

FIND_LAST_COMMA_REGEX = re.compile(r",(?=[^.]*$)")

def create_prepared_eoir_data(raw_data_path, prepared_data_path, filter_func):
  file_paths = filter(filter_func, list(raw_data_path.glob('*.csv')))

  for csv_to_read in file_paths:
    create_prepared_eoir_data_file(csv_to_read, prepared_data_path)

def create_prepared_eoir_data_file(csv_to_read, prepared_data_path):
  table_schema = get_table_schema(PROGRAM_DIR / SCHEMA_FILE)
  name = os.path.basename(csv_to_read)[:-4]

  prepared_data_path.mkdir(parents=True, exist_ok=True)

  with open(prepared_data_path / f'{name}.csv', 'w') as write_csv_file:
    with codecs.open(csv_to_read, 'r', 'utf-8-sig') as read_csv_file:
        print(f'Preparing csv: {name}')
        ## Remove any NULL characters (\0)
        ## Also, some CSV's have lines ending in "," before the return/newline, which end up escaping the last entry, so strip those with a regex
        reader = csv.reader(
          (FIND_LAST_COMMA_REGEX.sub('', line.replace('\0', ' ').replace('"', '')) for line in read_csv_file),
          delimiter='\t',
          skipinitialspace=True
        )
        writer = csv.writer(write_csv_file)
        fields = next(reader)
        writer.writerow(fields)
        all_keys_for_table = dict.fromkeys(table_schema[name].keys())
        for row in reader:
          cols = {
            **(all_keys_for_table),
            **(dict(zip(fields, row)))
          }
              
          writer.writerow(cols.values())

def get_table_schema(schema_csv):
  table_schema = {}
  
  with open(schema_csv, 'r') as csvfile:
      data = csv.reader(csvfile, delimiter='\t')
      next(data)
      for row in data:
          if row[0] in table_schema:
              table_schema[row[0]][row[1]] = row[2]
          else: 
              table_schema[row[0]] = {
                  row[1]: row[2]
              }

  return table_schema

def create_tables_if_not_exists(engine, prepared_data_path, filter_func):
  file_paths = filter(filter_func, list(prepared_data_path.glob('*.csv')))
  table_schema = get_table_schema(PROGRAM_DIR / SCHEMA_FILE)

  for csv_to_load in file_paths:
      trans = None         
      name = os.path.basename(csv_to_load)[:-4]

      table = create_table_if_not_exists(name, table_schema, engine)

def create_table_if_not_exists(table_name, table_schema, engine):
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

  table_cols = list(map(column_creator, zip(table_schema[table_name].keys(), table_schema[table_name].values())))
  
  table = Table(
      table_name, 
      metadata, 
      *table_cols
  )

  if engine.dialect.has_table(engine, table_name):
      table.drop()
  
  table.create()

  return table

def upload_csv_files(path_to_files, filter_func):
  file_paths = filter(filter_func, list(path_to_files.glob('*.csv')))
  conn = pg8000.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWRORD, database=DB_NAME)

  for csv_file in file_paths:
    with open(csv_file, 'rb') as csv_reader:
      name = os.path.basename(csv_file)[:-4]
      conn = pg8000.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWRORD, database=DB_NAME)
      cursor = conn.cursor()
      try:
        print(f'Uploading {csv_file}')
        cursor.execute(f'COPY "{name}" FROM STDIN WITH (format csv, header)', stream = csv_reader)
        cursor.close()
        conn.commit()
        conn.close()
      except pg8000.core.ProgrammingError as e:
        print(f'Failed to upload {csv_file}', e)
        ## If there's a ProgrammingError exception during copy, closing the cursor
        ## doesn't seem to rollback the transaction, need to disconnect and reconnect
        cursor.close()
        conn.commit();
        conn.close()

if __name__ == "__main__":
    main()

