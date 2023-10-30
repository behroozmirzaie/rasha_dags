import datetime as dt

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'behrooz',
    'start_date': dt.datetime.now(),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'schedule_interval': dt.timedelta(minutes=1),
}

dag = DAG(
    'retrieve_and_pass_sql_query',
    default_args=default_args,
    schedule_interval='@daily',
)


def get_postgres_data():
    conn = psycopg2.connect(host='postgresql', database='data_source', user='postgres', password='rasha_password')
    cur = conn.cursor()
    cur.execute('SELECT * FROM taxi_trips limit 100')
    rows = cur.fetchall()
    return rows


def show_rows(rows):
    for row in rows:
        print(row)


get_postgres_data_operator = PythonOperator(
    task_id='get_postgres_data_operator',
    python_callable=get_postgres_data,
    dag=dag,
)

show_rows_operator = PythonOperator(
    task_id='show_rows_operator',
    python_callable=show_rows,
    dag=dag,
)

get_postgres_data_operator >> show_rows_operator
