import logging
import uuid
from datetime import datetime, timedelta

import psycopg2
import requests
from croniter import croniter
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
__config_table__ = 'cron_config'

__csv_service_url__ = Variable.get("data-exhaust-service-url")
__csv_service_token__ = Variable.get("data-exhaust-service-token")
__data_exhaust_api_org_id__ = Variable.get("data-exhaust-api-org-id")
__submit_api_id__ = Variable.get("data-export-exhaust-submit-api-id")
__submit_api_ver__ = Variable.get("data-export-exhaust-submit-api-ver")
__submit_api_requestedBy__ = Variable.get(
    "data-export-exhaust-submit-api-user-id")
__submit_api_requestedChannel__ = Variable.get(
    "data-export-exhaust-submit-api-channel-id")
__submit_api_output_format__ = 'csv'


def get_connection(uri=__db_uri__):
    '''
    Initiate db connection
    '''
    conn = psycopg2.connect(uri)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return cur, conn


def insert_requests(bot_id, start_date, end_date, state_id, dataset):
    '''
    Insert requests objects in the table 'job-request' ready to be picked up by 'process_job_requests()'
    '''
    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()
    query = f"""
            insert into "{__request_table__}" ("bot_id", "tag", "start_date", "end_date", "state_id", "dataset") 
            values ('{bot_id}','{str(uuid.uuid4())}','{start_date}','{end_date}','{state_id}','{dataset}')
        """
    cur.execute(query)
    conn.commit()
    logging.info(
        f"Total row affected in {__request_table__} table on {now}: {str(cur.rowcount)}")
    conn.close()


def get_cron_config():
    '''
    Fetch runtime config from 'cron_config' table
    '''
    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()
    query = f'SELECT * FROM "{__config_table__}"'
    cur.execute(query)
    config_objs = cur.fetchall()
    conn.commit()
    conn.close()
    logging.info(
        f"Total no of records present in {__config_table__} table on {now}: {len(config_objs)}")
    return config_objs


def join_cron_config(exhaust_config, cron_config):
    '''
    Join the runtime config with the static config that we have
    in the 'exhaust_config.py' then joining those objects with common
    field 'state_id'
    '''
    config = []
    for cron_config_obj in cron_config:
        cron_config_obj = dict(cron_config_obj)
        for exhaust_config_obj in exhaust_config:
            if cron_config_obj['state_id'] == exhaust_config_obj['state_id']:
                config.append({**exhaust_config_obj, **cron_config_obj})
    return config


def create_job_requests(**context):
    '''
    Prepare jobs and push it in job_request table so that it
    can be picked up and executed later
    '''
    dt = context['execution_date'].to_date_string()
    dt_string = datetime.strptime(dt, "%Y-%m-%d").date()
    try:
        cur, conn = get_connection()
    except psycopg2.InterfaceError:
        cur, conn = get_connection()

    cron_config = join_cron_config(exhaust_config, get_cron_config())
    for config in cron_config:
        # frequency - start_date and end_date
        if not croniter.is_valid(config['frequency']):
            raise Exception(
                f"Frequency crontab format in {__config_table__} table on {dt_string} is invalid")
        freq_to_datetime = croniter(config['frequency'], datetime.now())
        end_date = freq_to_datetime.get_next(
            ret_type=datetime).date() - timedelta(days=1)
        start_date = datetime.now().date() - timedelta(days=1)
        difference = (end_date - start_date).days
        query = f"""
            SELECT * FROM "{__request_table__}"
            where "status" != 'SUCCESS'
            and "state_id" = '{config['state_id']}' 
            and "bot_id" = '{config['bot_id']}' 
            and ABS("end_date" - "start_date") = '{difference}'
        """
        cur.execute(query)
        result = cur.fetchall()
        if not result:
            insert_requests(config['bot_id'], str(start_date), str(end_date),
                            config['state_id'], config['dataset'])
        else:
            logging.info(
                f"Pending request already exist with this frequency in {__request_table__} table on {dt_string} for bot {config['bot_id']} and state {config['state_id']}")
    conn.commit()
    conn.close()


def call_data_exhaust_submit_api(bot_id, dataset, tag, start_date, end_date):
    body = {
        'id': __submit_api_id__,
        'ver': __submit_api_ver__,
        'ts': datetime.now().isoformat(),
        'params': {
            'msgid': str(uuid.uuid4()),
        },
        'request': {
            'dataset': dataset,
            'tag': tag,
            'requestedBy': __submit_api_requestedBy__,
            'requestedChannel': __submit_api_requestedChannel__,
            'datasetConfig': {
                'type': dataset,
                'conversationId': bot_id,
                'startDate': start_date,
                'endDate': end_date,
            },
            'output_format': __submit_api_output_format__
        }
    }
    logging.info(f"Request: {body}")
    r = requests.post(f'{__csv_service_url__}/submit',
                      json=body,
                      headers={
                          'Authorization': f'Bearer {__csv_service_token__}',
                          'X-Channel-ID': __data_exhaust_api_org_id__,
                          'Content-Type': 'application/json'
                      })
    return r.status_code, r.json()


def update_status_request_id(cur, status, request_id, tag):
    query = f"""
        UPDATE "{__request_table__}" SET "status"='{status}', "request_id"='{request_id}' where "tag"='{tag}'
    """
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
    query = f"""
        SELECT * FROM "{__request_table__}" 
        where "request_id" is null 
        and "end_date" <= '{str(datetime.now().date())}'
    """
    cur.execute(query)
    pending_requests = cur.fetchall()
    for req in pending_requests:
        req = dict(req)
        status_code, response = call_data_exhaust_submit_api(
            req['bot_id'], req['dataset'], req['tag'], str(req['start_date']), str(req['end_date']))
        if status_code == 200:
            if response['result']['status'] == 'SUBMITTED':
                update_status_request_id(
                    cur, response['result']['status'], response['result']['requestId'], req['tag'])
                logging.info(
                    f"Request of tag {req['tag']} on {dt_string} for bot {req['bot_id']} and state {req['state_id']} submitted success")
            else:
                logging.info(
                    f"Request of tag {req['tag']} on {dt_string} for bot {req['bot_id']} and state {req['state_id']} error")
        else:
            logging.error(
                f"Request of tag {req['tag']} on {dt_string} for bot {req['bot_id']} and state {req['state_id']} submitted failed")

    conn.commit()
    conn.close()


# cleanup
if __name__ == '__main__':
    class D:
        def __init__(self, date=datetime.now().date()):
            self.date = date

        def to_date_string(self):
            return str(self.date)
    create_job_requests(execution_date=D())
    process_job_requests(execution_date=D())
