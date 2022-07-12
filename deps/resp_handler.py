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

__read_api_url__ = '{{host}}/dataset/v1/request/read'


def get_connection(uri=__db_uri__):
    '''
    Initiate db connection
    '''
    conn = psycopg2.connect(uri)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return cur, conn


def call_data_exhaust_read_api(tag, request_id):
    '''
    call data_exhaust_read_api for fetching and saving csv link
    '''
    query_param = {
        'request_id': request_id
    }
    r = requests.get(f'{__read_api_url__}/{tag}', params=query_param)
    return r.status_code, r.json()


def update_status_csv(cur, status, request_id, tag):
    '''
    update column 'status' and 'csv' of 'job_request' table
    '''
    query = """UPDATE "{}" SET "status"='{}', "csv"='{}' where "tag"='{}'""".format(
        __request_table__, status, csv, tag)
    cur.execute(query)


def handle_read_requests(**context):
    '''
    handler of matured requests
    '''
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    report_date = dt_string - relativedelta(days=1)

    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()
    query = """SELECT * FROM "{}" where "request_id" is not null and "status" = '{}' and "end_date" <= '{}'""".format(
        __request_table__, 'SUBMITTED', datetime.now())
    cur.execute(query)
    matured_requests = cur.fetchall()

    for req in matured_requests:
        status_code, response = call_data_exhaust_read_api(
            req['tag'], req['request_id'])
        if status_code == 200:
            if response['status'] == 'SUCCESS':
                update_status_csv(
                    cur, response['status'], response['downloadUrl'][0])
                logging.info(
                    f"Response of tag {req['tag']} on {dt_string} for bot {req['bot']} and state {req['state_id']} saved success")
            else:
                logging.info(
                    f"Response of tag {req['tag']} on {dt_string} for bot {req['bot']} and state {req['state_id']} not ready yet")
        else:
            logging.error(
                f"Request of tag {req['tag']} on {dt_string} for bot {req['bot']} and state {req['state_id']} saved failed")

    conn.commit()
    conn.close()
