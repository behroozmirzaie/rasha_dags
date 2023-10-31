import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import psycopg2

default_args = {
    'owner': 'behrooz',
    'start_date': dt.datetime.now(),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'schedule_interval': dt.timedelta(minutes=1),
    'conn_id': 'get_postgres_data_to_s3_parquet'
}


def print_something():
    conn = psycopg2.connect(host='postgresql', database='data_source', user='postgres', password='rasha_password')
    cur = conn.cursor()
    cur.execute('SELECT * FROM taxi_trips limit 110')
    rows = cur.fetchall()
    print(rows)
    return rows


with DAG(
        default_args=default_args,
        dag_id="postgres_operator_dag",
        start_date=dt.datetime.now(),
        schedule_interval="@daily",

) as dag:
    show_data_from_table = PostgresOperator(
        postgres_conn_id="postgres_data_source_one",
        database="data_source",
        task_id="get_data_from_table",
        sql="""
                SELECT * FROM fatemeh;
              """,
    )
    print_something = PythonOperator(default_args=default_args, python_callable=print_something,
                                     task_id="python_task_id")
