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

# from .template_dict_store import Dict


logger = logging.getLogger()
logger.setLevel(logging.INFO)
now = datetime.now()

#################################################
#               Configs                         #
#################################################
__db_uri__ = Variable.get("main-db")
__request_table__ = 'job_request'
__response_table__ = 'response'
__config_table__ = 'cron_config'

__submit_api_url__ = '{{host}}/dataset/v1/request/submit'
__submit_api_id__ = 'ekstep.analytics.job.request.submit'
__submit_api_ver__ = '1.0'
__submit_api_requestedBy__ = 'user_id'
__submit_api_requestedChannel__ = '01309282781705830427'
__submit_api_output_format__ = 'csv'


def get_connection(uri=__db_uri__):
    '''
    Initiate db connection
    '''
    conn = psycopg2.connect(uri)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return cur, conn


def insert_requests(bot, start_date, end_date, state_id, dataset, job_type):
    '''
    Insert requests objects in the table 'job-request' ready to be picked up by 'process_job_requests()'
    '''
    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    query = 'insert into "{}" ("bot", "tag", "start_date", "end_date", "state_id", "dataset", "job_type") values (\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\')' \
        .format(__request_table__, bot, str(uuid.uuid4()), start_date, end_date, state_id, dataset, job_type)
    cur.execute(query)
    conn.commit()
    logging.info(
        f"Total row affected in {__table_name__} table on {dt_string}: {str(cur.rowcount)}")
    conn.close()


def get_cron_config():
    '''
    Fetch runtime config from 'cron_config' table
    '''
    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    query = 'SELECT * FROM "{}" where "job_type" != \'csv-process\''.format(
        __config_table__)
    cur.execute(query)
    config_objs = cur.fetchall()
    conn.commit()
    conn.close()
    logging.info(
        f"Total no of records present in {__table_name__} table on {dt_string}: {len(config_objs)}")
    return config_objs


def join_cron_config(exhaust_config, cron_config):
    '''
    Join the runtime config with the static config that we have
    in the 'exhaust_config.py' then joining those objects with common
    field 'state_id'
    '''
    for cron_config_obj in cron_config:
        for exhaust_config_obj in exhaust_config:
            if cron_config_obj['state_id'] == exhaust_config_obj['state_id']:
                cron_config_obj |= exhaust_config_obj
    return cron_config


def create_job_requests(**context):
    '''
    Prepare jobs and push it in job_request table so that it
    can be picked up and executed later
    '''
    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    report_date = dt_string - relativedelta(days=1)

    cron_config = join_cron_config(exhaust_config, get_cron_config())
    for config in cron_config:
        # frequency - start_date and end_date
        if not croniter.is_valid(config['frequency']):
            logging.error(
                f"Frequency crontab format in {__config_table__} table on {dt_string} is invalid")
        date_time = croniter(config['frequency'], datetime.now())
        end_date = date_time.get_next(datetime, ret_type=datetime)
        start_date = datetime.now()
        duration = end_date - start_date
        query = 'SELECT * FROM "{}" where "request_id" is null and "state_id" = \'{}\' and "bot" = \'{}\' and "start_date" - "end_date" = \'{}\''.format(
            __request_table__, config['state_id'], config['bot'], duration.days)
        cur.execute(query)
        result = cur.fetchall()
        if len(result) == 0:
            insert_requests(config['bot'], start_date, end_date,
                            config['state_id'], config['dataset'], config['job_type'])
        else:
            logging.info(
                f"Request already exist with this frequency in {__request_table__} table on {dt_string} for bot {config['bot']} and state {config['state_id']}")
    conn.commit()
    conn.close()


def call_data_exhaust_submit_api(bot, dataset, tag, type, start_date, end_date):
    query_param = {
        'id': __submit_api_id__,
        'ver': __submit_api_ver__,
        'ts': datetime.now().isoformat(),
        'params': {
            'msgid': uuid.uuid4(),
        },
        'request': {
            'dataset': dataset,
            'tag': tag,
            'requestedBy': __submit_api_requestedBy__,
            'requestedChannel': __submit_api_requestedChannel__,
            'datasetConfig': {
                'type': dataset,
                'conversationId': bot,
                'startDate': start_date,
                'endDate': end_date,
            },
            'output_format': __submit_api_output_format__
        }
    }
    r = requests.get(__submit_api_url__, params=query_param)
    return r.status_code, r.json()


def update_status_request_id(cur, status, request_id, tag):
    query = """UPDATE "{}" SET "status"='{}', "request_id"='{}' where "tag"='{}'""".format(
        __request_table__, status, request_id, tag)
    cur.execute(query)


def process_job_requests(**context):
    '''
    Process inserted request - call submit api for them
    '''
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    report_date = dt_string - relativedelta(days=1)

    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()
    query = 'SELECT * FROM "{}" where "request_id" is not null'.format(
        __request_table__)
    cur.execute(query)
    pending_requests = cur.fetchall()

    for req in pending_requests:
        status_code, response = call_data_exhaust_submit_api(
            req['bot'], req['dataset'], req['tag'], req['job_type'], req['start_date'], req['end_date'])
        if status_code == 200:
            if response['status'] == 'SUBMITTED':
                update_status_request_id(
                    cur, response['status'], response['requestId'], req['tag'])
                logging.info(
                    f"Request of tag {req['tag']} on {dt_string} for bot {config['bot']} and state {config['state_id']} submitted success")
            else:
                logging.info(
                    f"Request of tag {req['tag']} on {dt_string} for bot {config['bot']} and state {config['state_id']} error")
        else:
            logging.error(
                f"Request of tag {req['tag']} on {dt_string} for bot {config['bot']} and state {config['state_id']} submitted failed")

    conn.commit()
    conn.close()
