from airflow import DAG
from airflow.operators.python import PythonOperator
import airflow

def print_hello():
    print("Hello, world!")


default_args = {
    'start_date': airflow.utils.dates.days_ago(2),
    'schedule_interval': '@minute',
}

with DAG('print_hello_dag', default_args=default_args) as dag:
    print_hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    print_hello_task
