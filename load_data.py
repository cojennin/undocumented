import os
import codecs
import csv
from pathlib import Path
import subprocess
from prompt_toolkit import prompt
import shutil
import zipfile
import urllib3

http = urllib3.PoolManager()

EOIR_ZIP_URL = "https://fileshare.eoir.justice.gov/FOIA-TRAC-Report.zip"

EOIR_ZIP = "FOIA-TRAC-Report.zip"
EOIR_DATA = "eoir_data"
EOIR_DATA_PREPARED = "eoir_data_prepared"
SCHEMA_FILE = "schema.csv"
PROGRAM_DIR = Path(os.path.dirname(os.path.realpath(__file__)))

def main():
  ## First check if the data or prepared data directories exists, and if they do, should we delete them?
  # if eoir_data_prepared_exists() or eoir_data_exists():
  #   should_remove_data = is_prompt_yes(prompt('EOIR data already exists, delete it? (Y/n):'))
  #   if should_remove_data:
  #     remove_eoir_data()
  #     remove_eoir_prepared_data()

  ## If the zip file doesn't exist, should we download it? 
  if eoir_zip_exists():
    should_fetch_eoir_zip = is_prompt_yes(prompt('EOIR zip file eixsts, delete and fetch it from the EOIR website? (N/y): '))
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
    should_overwrite_data = is_prompt_yes(prompt('EOIR data already exists, overwrite it with new data zip file? (N/y) '))
    if should_overwrite_data:
      remove_eoir_data()
      unzip_file((PROGRAM_DIR / EOIR_ZIP), (PROGRAM_DIR / EOIR_DATA))
  else:
    unzip_file((PROGRAM_DIR / EOIR_ZIP), (PROGRAM_DIR / EOIR_DATA))
  
  # Prepare the CSV data for `COPY` into database
  # All rows have to have values for the header's for `COPY` command to work in POSTGRES
  if eoir_data_prepared_exists():
    should_overwrite_prepared_data = is_prompt_yes(prompt('Prepared EOIR data already exists, overwrite it with new data? (Y/n) '))
    if should_overwrite_prepared_data:
      remove_eoir_prepared_data()
      print('Preparing EOIR data, this can take a while...')
      create_prepared_eoir_data(PROGRAM_DIR / EOIR_DATA, PROGRAM_DIR / EOIR_DATA_PREPARED, lambda path: ".csv" in str(path) and "EOIRDB_Schema" not in str(path))
  else:
    print('Preparing EOIR data, this can take a while...')
    create_prepared_eoir_data(PROGRAM_DIR / EOIR_DATA, PROGRAM_DIR / EOIR_DATA_PREPARED, lambda path: ".csv" in str(path) and "EOIRDB_Schema" not in str(path))

def is_prompt_yes(answer):
  return answer.lower().strip() == 'y'

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

def create_prepared_eoir_data(raw_data_path, prepared_data_path, filter_func):
  file_paths = filter(filter_func, list(raw_data_path.glob('*.csv')))
  table_schema = get_table_schema(PROGRAM_DIR / SCHEMA_FILE)

  prepared_data_path.mkdir(parents=True, exist_ok=True)

  for csv_to_read in file_paths:
    name = os.path.basename(csv_to_read)[:-4]

    with open(prepared_data_path / f'{name}.csv', 'w') as write_csv_file:
      with codecs.open(csv_to_read, 'r', 'utf-8') as read_csv_file:
          print(f'Preparing csv: {name}')
          reader = csv.reader((line.replace('\0', ' ') for line in read_csv_file), delimiter='\t')
          writer = csv.writer(write_csv_file)
          fields = next(reader)
          writer.writerow(fields)
          index = 0
          all_keys_for_table = dict.fromkeys(table_schema[name].keys())
          for row in reader:
              cols = {
                **(all_keys_for_table),
                **(dict(zip(fields, row)))
              }
                  
              writer.writerow(cols.values())
              index += 1

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

if __name__ == "__main__":
    main()

