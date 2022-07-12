from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from deps.config.exhaust_config import config
from deps.requester import create_job_requests, process_job_requests

default_args = {
    "owner": "radhay",
    "depends_on_past": True,
    "start_date": datetime(year=2022, month=3, day=11),
    "email": ["radhay@samagragovernance.in"],

    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('request-creator', default_args=default_args, schedule_interval='0 9 * * *',
          max_active_runs=1, catchup=False)

start = DummyOperator(task_id="start", dag=dag)

create_requests = PythonOperator(
    task_id="create_requests",
    dag=dag,
    provide_context=True,
    python_callable=create_job_requests)

process_requests = PythonOperator(
    task_id="process_requests",
    dag=dag,
    provide_context=True,
    python_callable=process_job_requests)

end = DummyOperator(task_id="end", dag=dag)

start >> create_requests >> process_requests >> end
