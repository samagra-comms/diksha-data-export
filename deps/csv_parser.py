import requests
import json
import psycopg2
import logging
import time
import uuid
from datetime import datetime
from dateutil.relativedelta import relativedelta
from psycopg2.extras import RealDictCursor
from airflow.models import Variable
from deps.config.exhaust_config import config as exhaust_config
from croniter import croniter
import csv

# from .template_dict_store import Dict


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

__path_store_csv__ = f'/tmp/{uuid.uuid4()}'


def get_connection(uri=__db_uri__):
    '''
    Initiate db connection
    '''
    conn = psycopg2.connect(uri)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return cur, conn


def dump_data_in_uci_response_exhaust_table(curr, data):
    '''
    dump processed data in 'uci_response_exhaust' table
    '''
    query = 'insert into "{}" values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(
        __uci_response_exhaust_table__)
    cur.executemany(query, data)
    logging.info(
        f"Total row affected in {__uci_response_exhaust_table__} table on {dt_string}: {str(cur.rowcount)}")


def update_is_csv_processed(cur, tag):
    query = """UPDATE "{}" SET "is_csv_processed" = true where "tag"='{}'""".format(
        __request_table__, tag)
    cur.execute(query)


def download_csv(link):
    r = requests.get(link, stream=True)
    path = f'{__path_store_csv__}/{uuid.uuid4()}.csv'
    with open(f'{__path_store_csv__}/{filename}', "wb") as csv:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                csv.write(chunk)
    return path


def fix_csv_data(data):
    '''
    Apply some regex to process data in required form
    this function expect data without header row
    '''
    for i in range(0, len(data)):
        if data[i][5] == 'mcq':
            lst = re.findall(r'text\\\":\\\"(.+?)\\', data[i][11])
            lst = [x.strip() for x in lst]
            data[i][11] = ';'.join(lst)
        else:
            data[i][11] = ''
        data[i][14] = True if data[i][14] == 'true' else False
        data[i][12] = re.search(
            r'\[{(.*?):\\\"(.*?)\\\"(.*?)\]', data[i][12]).group(2).strip()
    return data


def get_csv_data(path):
    arr = []
    with open(path, mode='r') as f:
        csv_file = csv.reader(f)
        for lines in csv_file:
            arr.append(lines)
    return arr


def create_uci_response_exhaust_table_if_not_exist(curr):
    query = f'''CREATE TABLE IF NOT EXISTS {__uci_response_exhaust_table__} (
        message_id text,
        conversation_id text,
        conversation_name text,
        device_iD text,
        question_id text,
        question_type text,
        question_title text,
        question_description text,
        question_duration text,
        question_score text,
        question_max_score text,
        question_options text,
        question_response text,
        x_path text,
        eof boolean,
        timestamp TIMESTAMPTZ
    )'''
    cur.execute(query)


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
    query = 'SELECT * FROM "{}" where "csv" is not null and "is_csv_processed" = false'.format(
        __request_table__)
    cur.execute(query)
    unprocessed_csv = cur.fetchall()

    for csv in unprocessed_csv:
        config = [d for d in exhaust_config if d['state_id'] == state_id]
        if len(config) == 0:
            logger.error(
                'Invalid state_id passed to create_response_table_if_not_exist() method')
        try:
            cur_state, conn_state = get_connection(
                config[0]['db_credentials']['uri'])
        except psycopg2.InterfaceError:
            cur_state, conn_state = get_connection(
                config[0]['db_credentials']['uri'])

        path = download_csv(csv)
        data = get_csv_data(path)
        create_uci_response_exhaust_table_if_not_exist(cur_state)

        fixed_data = fix_csv_data(data)

        dump_data_in_uci_response_exhaust_table(cur_state, fixed_data)

        conn_state.commit()
        conn_state.close()
    conn.commit()
    conn.close()
