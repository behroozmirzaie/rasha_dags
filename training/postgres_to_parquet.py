import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
import boto3
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum
import duckdb

default_args = {
    'start_date': pendulum.datetime(2023, 10, 29, tz="CET"),
    'retries': 1,
}

dag = DAG(
    'get_postgres_data_to_s3_parquet',
    default_args=default_args,
    schedule_interval='@daily',
)


def get_postgres_data():
    get_data = PostgresOperator(
        postgres_conn_id="postgres_data_source_one",
        task_id="get_information",
        sql="""
                    Select * FROM FATEMEH;
                  """,
    )
    print(get_data)


# def convert_to_duckdb(rows):
#     conn = duckdb.connect()
#     cur = conn.cursor()
#     cur.execute('CREATE TABLE my_table (id INT, name TEXT, age INT)')
#
#     for row in rows:
#         cur.execute('INSERT INTO my_table VALUES (%s, %s, %s)', row)
#
#     conn.commit()
#     conn.close()


# def save_to_s3(duckdb_filename, s3_key):
#     s3 = boto3.client('s3')
#     s3.upload_file(duckdb_filename, 'my-bucket', s3_key)
#
#
# get_postgres_data_task = PythonOperator(
#     task_id='get_postgres_data',
#     python_callable=get_postgres_data,
#     dag=dag,
# )
#
# convert_to_duckdb_task = PythonOperator(
#     task_id='convert_to_duckdb',
#     python_callable=convert_to_duckdb,
#     dag=dag,
# )
#
# save_to_s3_task = S3FileTransformOperator(
#     task_id='save_to_s3',
#     s3_bucket='',
#     s3_key='my_table.parquet',
#     aws_conn_id='aws_default',
#     dag=dag,
# )

# get_postgres_data_task >> convert_to_duckdb_task >> save_to_s3_task
get_postgres_data
