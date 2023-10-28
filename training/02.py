import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_world():
    print('this should be run every minute')


default_args = {
    'owner': 'Behrooz',
    'start_date': dt.datetime(2023, 10, 26),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG(dag_id='airflow_tutorial_v02',
         default_args=default_args,
         schedule_interval='* * * * *',
         ) as dag:
    print_world = PythonOperator(task_id='print_every_minute',
                                 python_callable=print_world)
