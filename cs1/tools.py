""" c1.tools

    Define functions to be used in the Case Study 1 Jupyter Notebooks.
"""
from cmd import Cmd
from datetime import date, datetime, timedelta
from dateutil.parser import parse
from ftplib import FTP_TLS as FTP
from glob import glob
import os
from pathlib import Path
from pprint import pprint as pp
import re
import shutil
import sys
from traceback import print_exc
import warnings
from warnings import warn
from zipfile import ZipFile

warnings.simplefilter("ignore")

from jellyfish import levenshtein_distance as similar
import mysql.connector
from numpy import *
import pandas as pd
import requests

MAX_COLS = 14

BASE_DIR = Path(os.curdir).resolve().absolute()
DATA_DIR = BASE_DIR / 'data'
TEST_DIR = BASE_DIR / 'test'
ARCHIVE  = BASE_DIR / 'archive'
DOWNLOAD = BASE_DIR / 'download'
CLEAN_DIR = BASE_DIR / 'clean'
INDEX_TEXT = BASE_DIR / 'aws_index.txt'
CSV_DIR = BASE_DIR / 'csv'
HEADER_FILE = BASE_DIR / 'header.csv'
STAGED_DIR = BASE_DIR / 'staged'

DT_ZERO = timedelta(0, 0, 0, 0, 0, 0)

COLUMNS = dict()
COLUMNS['ID'] = ['01 - Rental Details Rental ID', 'ride_id', 'trip_id']
COLUMNS['Start Time'] = ['01 - Rental Details Local Start Time', 'started_at', 'start_time', 'starttime']
COLUMNS['End Time'] = ['01 - Rental Details Local End Time', 'ended_at', 'end_time', 'stoptime']
COLUMNS['Bike ID'] = ['01 - Rental Details Bike ID', 'bikeid']
COLUMNS['Duration'] = ['01 - Rental Details Duration In Seconds Uncapped', 'tripduration']
COLUMNS['From Station ID'] = ['from_station_id', 'start_station_id', '03 - Rental Start Station ID']
COLUMNS['To Station ID'] = ['02 - Rental End Station ID', 'end_station_id', 'to_station_id']
COLUMNS['User Type'] = ['User Type', 'member_casual', 'usertype']
COLUMNS['Gender'] = ['Member Gender', 'gender']
COLUMNS['Birth Year'] = ['05 - Member Details Member Birthday Year', 'birthyear', 'birthday']
COLUMNS['To Station Name'] = ['02 - Rental End Station Name', 'end_station_name', 'to_station_name']
COLUMNS['From Station Name'] = ['03 - Rental Start Station Name', 'from_station_name', 'start_station_name']
COLUMNS['End Latitude'] = ['end_lat']
COLUMNS['End Longitude'] = ['end_lng']
COLUMNS['Start Latitude'] = ['start_lat']
COLUMNS['Start Longitude'] = ['start_lng']
COLUMNS['Bike Type'] = ['rideable_type']

COLS_LIST = COLUMNS.keys()

USER_VALS = dict()
USER_VALS['M'] = ['Subscriber', 'member']
USER_VALS['C'] = ['Customer', 'casual']
USER_VALS['D'] = ['Dependent']

def pwd():
    return str(Path(os.curdir).resolve().absolute())

def cd(p: Path):
    os.chdir(p)
    return pwd()

def columnize(l):
    Cmd().columnize(l)

def public(obj):
    columnize([s for s in dir(obj) if not s.startswith('_')])

def head(p):
    with p.open() as f:
        for i in range(20):
            print("'" + f.readline().rstrip('\n') + "'")

def geo_deg_2_feet(d):
    return d*1000/9*3280.4

def like(s1, s2):
    return similar(s1, s2) < 2

def hilite_src_lines(obj):
    codeStr = inspect.getsource(obj)
    hilite_params = { "code": codeStr }
    return requests.post(HILITE_ME, hilite_params).text

def reverse_lookup(d:dict, s:str):
    result = None
    for i in d.items():
        if i[0] == s:
            return i[0]
        if type(i[1]) is str:
            if i[1] == s:
                return i[0]
        elif type(i[1]) is list:
            for t in i[1]:
                if t == s:
                    return i[0]
    return None

def rev_lookup(s):
    return reverse_lookup(COLUMNS, s)

def download_data():
    if not DOWNLOAD.exists():
        DOWNLOAD.mkdir()
    cur_dir = pwd()
    os.chdir(DOWNLOAD)
    for url in [s for s in re.compile('<(.*)>').findall(INDEX_TEXT.read_text()) if s.endswith('.zip')]:
        (DOWNLOAD / url.split('/')[-1]).write_bytes(requests.get(url).content)

def extract_data():
    cur_dir = pwd()
    if not DATA_DIR.exists():
        DATA_DIR.mkdir()
    cd(DATA_DIR)
    for f in glob(f'{str(ARCHIVE)}/*.zip'):
        z = ZipFile(f)
        csv_files = [info.filename for info in z.filelist if info.filename.endswith('.csv') and not info.filename.startswith('_')]
        for csv in csv_files:
            z.extract(csv)
    cd(cur_dir)

def move_zip_files():
    if not ARCHIVE.exists():
        ARCHIVE.mkdir()
    for p in [Path(f) for f in glob(f'{str(DOWNLOAD)}/*.zip')]:
        src = str(p)
        dest = str(ARCHIVE / p.name)
        shutil.move(src, dest)

def refresh_data():
    download_data()
    extract_data()
    move_zip_files()

def list_files():
    csv_files = [Path(s).absolute() for s in glob('**', recursive=True) if s.endswith('.csv')]
    station_files = [p for p in csv_files if 'Station' in p.name]
    trip_files = list(set(csv_files).difference(station_files))
    return csv_files, station_files, trip_files

def list_trip_files(src=DATA_DIR):
    if type(d) is str:
        d = Path(d)
    return [d / s for s in glob('**', root_dir=src, recursive=True) if s.endswith('.csv') and not 'Station' in s.split('/')[-1]]

def list_station_files(src=DATA_DIR):
    pass

CSV_FILES, STATION_FILES, TRIP_FILES = list_files()

def read_trip_csv_frame(f):
    h = pd.read_csv(HEADER_FILE)
    ff = pd.read_csv(f)
    ff.columns = h.columns
    return pd.concat([h, ff])

def unique_values(s):
    v = list()
    for f in list_trip_files():
        df = pd.read_csv(f)
        for u in df[s].dropna().unique():
            v.append(u)
    v = list(set(v))
    v.sort()
    if '' in v:
        v = v.remove('')
    return v

def find_trip_cols():
    trip_cols = list()
    for p in list_trip_files():
        with p.open() as f:
            new_list = [s.strip() for s in next(f).strip().split(',')]
    #         while len(new_list) < MAX_COLS:
    #             new_list.append('')
            trip_cols.extend(new_list)

    return list(set([str(L) for L in trip_cols]))

def unique_trip_cols():
    u = pd.Series(find_trip_cols()).unique()
    return [u[i] for i in range(len(u))]

def consist_cols(dest=DATA_DIR):
    for i, p in enumerate(list_trip_files()):
        print(f'{i=}')
        print(f'Processing {p}')
        lines = p.read_text().split('\n')
        print(f'Columns: {lines[0]}')
        C = lines[0].split(',')
        output = lines[0]
        for s in C:
            if s in COLS_LIST:
                s2 = s
            else:
                if s.startswith('"'):
                    s2 = '"' + reverse_lookup(COLUMNS, s.strip('"')) + '"'
                else:
                    s2 = reverse_lookup(COLUMNS, s)
        #         print(s2)
            output = output.replace(s, s2 if s2 else '')
        print(f'New Columns: {output}')
        print()
        (dest/p.name).write_text('\n'.join([output, *lines[1:]]))

def db_connect():
    expected_sql_state = None
    actual_sql_state = None
    output = ''
    cnx = None
    cursor = None
    user, password, host = (Path.home()/'.config'/'my.txt').read_text().split(':')
    try:
        cnx = mysql.connector.connect(user=user, password=password, host=host, database=user)
        cursor = cnx.cursor()
        print(f'Connected to database {user} at {host}')
    except mysql.connector.ProgrammingError as e:
        # print('Caught a ProgrammingError!')
        actual_sql_state = e.sqlstate
        output = 'Error: '
        if e.errno == 1049:
            output += e.msg
            expected_sql_state = '42000'
        elif e.errno == 1045:
            output += 'wrong password'
            expected_sql_state = '28000'
        elif e.errno == 1698:
            output += 'bad user name'
            expected_sql_state = '28000'
        else:
            print('Unknown Programming Error!')
            pp(e)
            print(f'{e.errno=}')
            print(f'{e.sqlstate=}')
            print(f'{e.msg=}')
        print(output)
        if actual_sql_state != expected_sql_state:
            print(f'Unexpected SQL state: {actual_sql_state}')
        return None
    except mysql.connector.InterfaceError as e:
        actual_sql_state = e.sqlstate
        output = 'Error: '
        if e.errno == 2003:
            output += e.msg
        else:
            print('Unknown Interface Error!')
            pp(e)
            print(f'{e.errno=}')
            print(f'{e.sqlstate=}')
            print(f'{e.msg=}')
        print(output)
        if actual_sql_state != expected_sql_state:
            print(f'Unexpected SQL state: {actual_sql_state}')
        return None
    return cnx, cursor

def ftp_connect():
    user, password, host = (Path.home()/'.config'/'ftp.txt').read_text().split(':')
    ftp = FTP(host=host)
    ftp.sendcmd(f'USER {user}')
    ftp.sendcmd(f'PASS {password}')
    return ftp



# def choose_name(names):
#     print("WARNING: Station has multiple names!")
#     print()
#     for i, n in enumerate(names):
#         print(f'{i}: {n}')
#     print()
#     i = input("Enter a name to use: ")
#     if i.isdigit():
#         return names[i]
#     else:
#         return i
