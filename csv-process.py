from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from deps.config.exhaust_config import config
from deps.csv_parser import process_csv

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

dag = DAG('csv-process', default_args=default_args, schedule_interval='0 23 * * *',
          max_active_runs=1, catchup=False)

start = DummyOperator(task_id="start", dag=dag)

csv_process = PythonOperator(
    task_id="read_requests",
    dag=dag,
    provide_context=True,
    python_callable=process_csv)

end = DummyOperator(task_id="end", dag=dag)

start >> csv_process >> end
