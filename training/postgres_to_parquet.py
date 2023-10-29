import datetime as dt

from airflow import DAG

from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'behrooz',
    'start_date': dt.datetime.now(),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'schedule_interval': dt.timedelta(minutes=1),
    'conn_id': 'get_postgres_data_to_s3_parquet'
}

with DAG(
        default_args=default_args,
        dag_id="postgres_operator_dag",
        start_date=dt.datetime.now(),
        schedule_interval="@daily",

) as dag:
    create_pet_table = PostgresOperator(
        postgres_conn_id="postgres_data_source_one",
        task_id="get_data_from_table",
        sql="""
                SELECT * FROM FATEMEH;
              """,
    )

    create_pet_table
