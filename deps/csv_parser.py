import csv
import itertools
import logging
import re
import uuid
from datetime import datetime

import psycopg2
import requests
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
__uci_response_exhaust_table__ = 'uci_response_exhaust'
__config_table__ = 'cron_config'

__path_store_csv__ = '/tmp/'


def get_connection(uri=__db_uri__):
    '''
    Initiate db connection
    '''
    conn = psycopg2.connect(uri)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return cur, conn


def insert_data_in_uci_response_exhaust_table(cur, conn, data):
    '''
    dump processed data in 'uci_response_exhaust' table
    '''
    query = '''
        insert into "{}" 
        ("message_id", "conversation_id", "conversation_name", "device_id", "question_id", "question_type", "question_title", "question_description", "question_duration", "question_score", "question_max_score", "question_options", "question_response", "x_path", "eof", "timestamp")
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    '''.format(__uci_response_exhaust_table__)
    chunk_size = 100000
    for i in range(0, len(data), chunk_size):
        data_to_insert = list(map(lambda x: x[:-1], data[i:i+chunk_size]))
        cur.executemany(query, data_to_insert)
    logging.info(
        f"Total row affected in {__uci_response_exhaust_table__} table on {now}: {str(cur.rowcount)}")
    conn.commit()


def update_is_csv_processed(cur, tag):
    query = """UPDATE "{}" SET "is_csv_processed" = TRUE where "tag"='{}'""".format(
        __request_table__, tag)
    cur.execute(query)


def get_csv_file_data(link):
    r = requests.get(link, stream=True)
    path = f'{__path_store_csv__}/{uuid.uuid4()}.csv'
    with open(path, "wb") as csv_file:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                csv_file.write(chunk)
    with open(path, mode='r') as f:
        content = f.read()
        content = re.sub(r'\n\n', r'', content)
        content = re.sub(r'\\"', r'"', content)
        content = re.sub(r'{(.*?)}}((,))', r'\g<1>}&!&', content)
        with open(f'{__path_store_csv__}/temp.csv', mode='w') as wf:
            wf.write(content)
        with open(f'{__path_store_csv__}/temp.csv', mode='r') as rf:
            csv_file = csv.reader(rf, quoting=csv.QUOTE_NONE, quotechar='"')
            data = [list(map(lambda x: x.strip('"'), l)) for l in csv_file]
    return data


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


def create_or_update_uci_response_exhaust_table(cur, conn):
    column_datatype_map = {
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
    }

    query = f'''
        SELECT EXISTS (
        SELECT FROM 
            information_schema.tables 
        WHERE 
            table_schema LIKE 'public' AND 
            table_type LIKE 'BASE TABLE' AND
            table_name = '{__uci_response_exhaust_table__}'
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
            table_name = '{__uci_response_exhaust_table__}'
        '''
        cur.execute(query)
        records = cur.fetchall()
        for r in records:
            column_datatype_map.pop(dict(r)['column_name'], None)
        if column_datatype_map:
            query = f'''
                ALTER TABLE "{__uci_response_exhaust_table__}" 
            '''
            for k, v in column_datatype_map.items():
                query += f'ADD COLUMN {k} {v}, '
            query = query[:-2]
            cur.execute(query)
    else:
        query = f'''CREATE TABLE "{__uci_response_exhaust_table__}" ('''
        for k, v in column_datatype_map.items():
            query += f'{k} {v}, '
        query = query[:-2]
        query += ')'
        cur.execute(query)
    conn.commit()


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
    query = 'SELECT * FROM "{}" where "csv" is not null and "is_csv_processed" = FALSE'.format(
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

        create_or_update_uci_response_exhaust_table(cur_state, conn_state)
        for csv_record in state_csv_records:
            csv_record = dict(csv_record)
            data = get_csv_file_data(csv_record['csv'])
            parsed_data = parse_csv_data(data[1:])

            insert_data_in_uci_response_exhaust_table(
                cur_state, conn_state, parsed_data)

            update_is_csv_processed(cur, csv_record['tag'])

        conn_state.commit()
        conn_state.close()
    conn.commit()
    conn.close()
