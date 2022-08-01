import csv
import itertools
import logging
import re
import uuid
from datetime import datetime

import psycopg2
import requests
import hashlib
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

__path_store_csv__ = '/tmp/'


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
    chunk_size = 100000
    for i in range(0, len(data), chunk_size):
        data_to_insert = data[i:i+chunk_size]
        cur.executemany(query, data_to_insert)
    logging.info(
        f"Total row affected in {tablename} table on {now}: {str(cur.rowcount)}")
    conn.commit()


def update_is_csv_processed(cur, tag):
    query = """UPDATE "{}" SET "is_csv_processed" = TRUE where "tag"='{}'""".format(
        __request_table__, tag)
    cur.execute(query)


def get_csv_file_data(link):
    r = requests.get(link, stream=True, headers={
        'Accept': 'text/csv'
    })
    if r.status_code == 200:
        path = f'{__path_store_csv__}/{uuid.uuid4()}.csv'
        with open(path, "wb") as csv_file:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    csv_file.write(chunk)
        with open(path, mode='r') as f:
            content = f.read()
            content = re.sub(r'\n\n+', r'', content)
            content = re.sub(r'\\"', r'"', content)
            content = re.sub(r'{(.*?)}}((,))', r'\g<1>}&!&', content)
            with open(f'{__path_store_csv__}/temp.csv', mode='w') as wf:
                wf.write(content)
            with open(f'{__path_store_csv__}/temp.csv', mode='r') as rf:
                csv_file = csv.reader(
                    rf, quoting=csv.QUOTE_NONE, quotechar='"')
                data = [list(map(lambda x: x.strip('"'), l)) for l in csv_file]
        data = list(map(lambda x: x[:-1], data))
        return data
    return None


def parse_csv_data(arr):
    '''
    Apply some regex to process data in required form
    this function expect data without header row
    '''
    for i in range(len(arr)):
        if arr[i][5] == 'mcq':
            lst = re.findall(r'\"text\":\"(.*?)\"', arr[i][11])
            arr[i][11] = ':'.join(lst)
        arr[i][14] = True if arr[i][14] == 'true' else False
        lst = re.findall(r'\"(.*?)\":(\"|\{\"text\":\")(.*?)\"', arr[i][12])
        arr[i][12] = ':'.join([x[2] for x in lst])
    return arr


def mark_for_redownload(cur, tag):
    '''
    update column 'status' and 'csv' of 'job_request' table so that
    csv should get re-downloaded due to expired link
    '''
    query = """UPDATE "{}" SET "status"='SUBMITTED', "csv"='' where "tag"='{}'""".format(
        __request_table__, tag)
    cur.execute(query)
    cur.commit()


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
    # hashing of "device_id" + "timestamp"
    for el in data:
        el.append(hashlib.sha256(
            f'{el[3]}{el[15]}'.encode('utf-8')).hexdigest())
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
    query = """SELECT * FROM "{}" where "status" = 'SUCCESS' and "csv" is not null and "is_csv_processed" = FALSE""".format(
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

            data = get_csv_file_data(csv_record['csv'])
            if not data:
                mark_for_redownload(cur, csv_record['tag'])
                continue
            parsed_data = parse_csv_data(data[1:])

            data_to_insert = filter_already_inserted_data(
                cur_state, parsed_data, tablename)

            # add config_id for tracing in future
            for el in data_to_insert:
                el.insert(0, csv_record['config_id'])

            insert_data_in_uci_response_exhaust_table(
                cur_state, conn_state, data_to_insert, tablename)

            update_is_csv_processed(cur, csv_record['tag'])

        conn_state.commit()
        conn_state.close()
    conn.commit()
    conn.close()
