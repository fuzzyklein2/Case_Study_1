from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from tools import *

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

def list_trip_files(src=DATA_DIR):
    cd(src)
    if type(src) is str:
        src = Path(src)
    value = [src / s for s in glob('**', recursive=True) if s.endswith('.csv') and not 'Station' in s.split('/')[-1]]
    cd(BASE_DIR)
    return value

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
