import logging
import os
import uuid
from datetime import datetime
from pathlib import Path

import psycopg2
import requests
from dateutil.relativedelta import relativedelta
from psycopg2.extras import RealDictCursor

from airflow.models import Variable

logger = logging.getLogger()
logger.setLevel(logging.INFO)
now = datetime.now()

#################################################
#               Configs                         #
#################################################
__db_uri__ = Variable.get("main-db")
__request_table__ = 'job_request'
__config_table__ = 'cron_config'

__csv_service_url__ = Variable.get("data-exhaust-service-url")
__csv_service_token__ = Variable.get("data-exhaust-service-token")
__data_exhaust_api_org_id__ = Variable.get("data-exhaust-api-org-id")

__path_store_csv__ = '/tmp/diksha/'


def get_connection(uri=__db_uri__):
    '''
    Initiate db connection
    '''
    conn = psycopg2.connect(uri)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return cur, conn


def call_data_exhaust_read_api(request_id, tag):
    '''
    call data_exhaust_read_api for fetching and saving csv link
    '''
    query_param = {
        'requestId': request_id
    }
    r = requests.get(f'{__csv_service_url__}/read/{tag}', params=query_param, headers={
        'Authorization': f'Bearer {__csv_service_token__}',
        'X-Channel-ID': __data_exhaust_api_org_id__,
        'Content-Type': 'application/json'
    })

    return r.status_code, r.json()


def update_status_csv(cur, status, csv, path, tag):
    '''
    update column 'status' and 'csv' of 'job_request' table
    '''
    query = """UPDATE "{}" SET "status"='{}', "csv"='{}', "file_path"='{}' where "tag"='{}'""".format(
        __request_table__, status, csv, path, tag)
    cur.execute(query)


def update_status(cur, status, tag):
    '''
    update column 'status' of 'job_request' table
    '''
    query = """UPDATE "{}" SET "status"='{}' where "tag"='{}'""".format(
        __request_table__, status, tag)
    cur.execute(query)


def get_csv_file(link):
    r = requests.get(link, stream=True, headers={
        'Accept': 'text/csv'
    })
    if r.status_code == 200:
        storage_path = Path(__path_store_csv__)
        if not storage_path.is_dir():
            os.mkdir(__path_store_csv__)
        path = f'{__path_store_csv__}/{uuid.uuid4()}.csv'
        with open(path, "wb") as csv_file:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    csv_file.write(chunk)
        return path
    return None


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
    query = """SELECT * FROM "{}" where "request_id" is not null and "status" = '{}'""".format(
        __request_table__, 'SUBMITTED')
    cur.execute(query)
    matured_requests = cur.fetchall()

    for req in matured_requests:
        req = dict(req)
        status_code, response = call_data_exhaust_read_api(
            req['request_id'], req['tag'])

        if status_code == 200:
            if response['result']['status'] == 'SUCCESS':
                path = get_csv_file(response['result']['downloadUrls'][0])
                if path:
                    # csv file stored
                    update_status_csv(
                        cur, response['result']['status'], response['result']['downloadUrls'][0], path, req['tag'])
                logging.info(
                    f"Response of tag {req['tag']} on {dt_string} for bot {req['bot_id']} and state {req['state_id']} saved success")
            elif response['result']['status'] == 'FAILED':
                update_status(cur, response['result']['status'], req['tag'])
                logging.info(
                    f"Response of tag {req['tag']} on {dt_string} for bot {req['bot_id']} and state {req['state_id']} does not contain any data")
            else:
                logging.info(
                    f"Response of tag {req['tag']} on {dt_string} for bot {req['bot_id']} and state {req['state_id']} not ready yet")
        else:
            logging.error(
                f"Request of tag {req['tag']} on {dt_string} for bot {req['bot_id']} and state {req['state_id']} saved failed")
    conn.commit()
    conn.close()
