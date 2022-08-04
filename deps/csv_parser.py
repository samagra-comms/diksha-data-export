import hashlib
import itertools
import logging
import re
from datetime import datetime
from pathlib import Path

import psycopg2
from dateutil.relativedelta import relativedelta
from psycopg2.extras import RealDictCursor

from airflow.models import Variable
from config.exhaust_config import config as exhaust_config

logger = logging.getLogger()
logger.setLevel(logging.INFO)
now = datetime.now()

#################################################
#               Configs                         #
#################################################
__db_uri__ = Variable.get("main-db")
__request_table__ = 'job_request'
# __uci_response_exhaust_table__ = 'uci_response_exhaust'
__config_table__ = 'cron_config'

__chunk_size__ = 100000


__column_datatype_map__ = {
    'config_id': 'int',
    'message_id': 'text',
    'conversation_id': 'text',
    'conversation_name': 'text',
    'device_id': 'text',
    'question_id': 'text',
    'question_type': 'text',
    'question_title': 'text',
    'question_description': 'text',
    'question_duration': 'text',
    'question_score': 'text',
    'question_max_score': 'text',
    'question_options': 'text',
    'question_response': 'text',
    'x_path': 'text',
    'eof': 'boolean',
    'timestamp': 'TIMESTAMPTZ',
    'hash_device_id_timestamp': 'text',
}


def get_connection(uri=__db_uri__):
    '''
    Initiate db connection
    '''
    conn = psycopg2.connect(uri)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return cur, conn


def insert_data_in_uci_response_exhaust_table(cur, conn, data, tablename):
    '''
    dump processed data in 'uci_response_exhaust' table
    '''
    query = '''
        insert into "{}"
        ({})
    '''.format(tablename, ','.join([f'"{x}"' for x in __column_datatype_map__.keys()]))
    query += "values (" + ','.join(['%s'] * len(__column_datatype_map__)) + ")"
    cur.executemany(query, data)
    logging.info(
        f"Total row affected in {tablename} table on {now}: {str(cur.rowcount)}")
    conn.commit()


def update_is_csv_processed(cur, tag):
    query = """UPDATE "{}" SET "is_csv_processed" = TRUE where "tag"='{}'""".format(
        __request_table__, tag)
    cur.execute(query)


def parse_line(line):
    line = re.sub(r'\\"', r'"', line)  # remove excess quotes
    arr = line.split(',')
    new_arr = []
    for i in range(len(arr) - 1):
        # handle commas in double quotes
        if arr[i].startswith('"') and not arr[i].endswith('"'):
            arr[i + 1] = arr[i] + arr[i + 1]
        else:
            new_arr.append(arr[i])
    new_arr.append(arr[-1])
    return parse_csv_arr(new_arr)


def parse_csv_arr(arr):
    '''
    Apply some regex to process data in required form
    this function expect data without header row
    '''
    for i, el in enumerate(arr):
        arr[i] = arr[i].strip('"')  # remove quotes
    if arr[5] == 'mcq':
        lst = re.findall(r'\"text\":\"((.|\n)*?)\"', arr[11])
        arr[11] = ':'.join(x[0] for x in lst)
    arr[14] = True if arr[14] == 'true' else False
    lst = re.findall(r'\"(.*?)\":(\"|\{\"text\":\")((.|\n)*?)\"', arr[12])
    arr[12] = ':'.join([x[2] for x in lst])
    del arr[-1]  # remove redundant last column '@timestamp' in csv
    # add hash of "device_id" + "timestamp"
    arr.append(hashlib.sha256(
        f'{arr[3]}{arr[15]}'.encode('utf-8')).hexdigest())
    return arr


def append_config_id_arr(arr, config_id):
    '''
    append config_id to each element of array
    '''
    for el in arr:
        el.insert(0, config_id)
    return arr


def mark_for_redownload(cur, conn, tag):
    '''
    update column 'status' and 'csv' of 'job_request' table so that
    csv should get re-downloaded due to expired link
    '''
    query = """UPDATE "{}" SET "status"='SUBMITTED', "csv"='', "file_path"='', "is_csv_processed"=FALSE where "tag"='{}'""".format(
        __request_table__, tag)
    cur.execute(query)
    conn.commit()


def create_or_update_uci_response_exhaust_table(cur, conn, tablename):
    column_datatype_map = __column_datatype_map__.copy()
    query = f'''
        SELECT EXISTS (
        SELECT FROM
            information_schema.tables
        WHERE
            table_schema LIKE 'public' AND
            table_type LIKE 'BASE TABLE' AND
            table_name = '{tablename}'
        )
    '''
    cur.execute(query)
    result = dict(cur.fetchone())
    if result['exists']:
        query = f'''
        SELECT
            column_name
        FROM
            information_schema.columns
        WHERE
            table_name = '{tablename}'
        '''
        cur.execute(query)
        records = cur.fetchall()
        for r in records:
            column_datatype_map.pop(dict(r)['column_name'], None)
        if column_datatype_map:
            query = f'''
                ALTER TABLE "{tablename}"
            '''
            for k, v in column_datatype_map.items():
                query += f'ADD COLUMN {k} {v}, '
            query = query[:-2]
            cur.execute(query)
            query = f'''CREATE UNIQUE INDEX IF NOT EXISTS hash_idx ON "{tablename}" ("hash_device_id_timestamp")'''
            cur.execute(query)
    else:
        query = f'''CREATE TABLE "{tablename}" ('''
        for k, v in column_datatype_map.items():
            query += f'{k} {v}, '
        query = query[:-2]
        query += ')'
        cur.execute(query)
        query = f'''CREATE UNIQUE INDEX IF NOT EXISTS hash_idx ON "{tablename}" ("hash_device_id_timestamp")'''
        cur.execute(query)
    conn.commit()


def filter_already_inserted_data(cur, data, tablename):
    """
    filter the data which is already inserted in db and only unique records should
    get inserted
    """
    hashes = [f"'{el[-1]}'" for el in data]
    # checking hash exist
    query = f'''
        select "hash_device_id_timestamp" from "{tablename}"
        where "hash_device_id_timestamp" in ({','.join(hashes)})
    '''
    cur.execute(query)
    records = cur.fetchall()
    # filtering
    already_exists_hashes = list(map(lambda x: dict(
        x)['hash_device_id_timestamp'], records))
    data = list(filter(lambda el: el[-1] not in already_exists_hashes, data))
    return data


def process_csv(**context):
    '''
    download the unprocessed csv then parse it then dump it into the tables
    '''
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    report_date = dt_string - relativedelta(days=1)

    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()
    query = """SELECT * FROM "{}" where "status" = 'SUCCESS' and "file_path" is not null and "is_csv_processed" = FALSE""".format(
        __request_table__)
    cur.execute(query)
    unprocessed_csv_records = cur.fetchall()
    state_group = itertools.groupby(
        unprocessed_csv_records, key=lambda e: dict(e)['state_id'])
    for state_id, state_csv_records in state_group:
        config = list(
            filter(lambda x: x['state_id'] == state_id, exhaust_config))
        assert len(
            config) > 0, f"No config available for state_id = {state_id} in exhaust_config"
        try:
            cur_state, conn_state = get_connection(
                config[0]['db_credentials']['uri'])
        except psycopg2.InterfaceError:
            cur_state, conn_state = get_connection(
                config[0]['db_credentials']['uri'])

        for csv_record in state_csv_records:
            csv_record = dict(csv_record)
            tablename = f"uci_response_exhaust:{csv_record['bot_id']}"

            create_or_update_uci_response_exhaust_table(
                cur_state, conn_state, tablename)

            path = Path(csv_record['file_path'])
            if not path.is_file():
                # file not found need to re-download
                mark_for_redownload(cur, conn, csv_record['tag'])
                continue

            data = []
            with open(csv_record['file_path'], mode='r') as f:
                actual_line = ''
                for i, line in enumerate(f):
                    if (i == 0):  # header row; ignore
                        continue

                    if line.startswith('ASSESS'):
                        if actual_line:
                            arr = parse_line(actual_line)
                            data.append(arr)
                        actual_line = line
                    else:
                        actual_line += line

                    if (len(data) == __chunk_size__):
                        data_to_insert = filter_already_inserted_data(
                            cur_state, data, tablename)
                        data_to_insert = append_config_id_arr(
                            data_to_insert, csv_record['config_id'])
                        insert_data_in_uci_response_exhaust_table(
                            cur_state, conn_state, data_to_insert, tablename)
                        data.clear()

                # last line
                if actual_line:
                    arr = parse_line(actual_line)
                    data.append(arr)

            # last chunk
            if data:
                data_to_insert = filter_already_inserted_data(
                    cur_state, data, tablename)
                data_to_insert = append_config_id_arr(
                    data_to_insert, csv_record['config_id'])
                insert_data_in_uci_response_exhaust_table(
                    cur_state, conn_state, data_to_insert, tablename)

            update_is_csv_processed(cur, csv_record['tag'])

        conn_state.commit()
        conn_state.close()
    conn.commit()
    conn.close()
