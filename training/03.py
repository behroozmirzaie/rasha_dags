import datetime as dt

from airflow import DAG

from airflow.providers.postgres.operators.postgres import PostgresOperator


def print_world():
    print('this should be run every minute')


default_args = {
    'owner': 'Behrooz',
    'start_date': dt.datetime.now(),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'schedule_interval': dt.timedelta(minutes=1),
}

with DAG(
        dag_id="postgres_operator_dag",
        start_date=dt.datetime.now(),
        schedule_interval="@daily",
) as dag:
    create_pet_table = PostgresOperator(
        conn_id="postgres_data_source_1",
        task_id="create_pet_table",
        sql="""
                CREATE TABLE IF NOT EXISTS pet (
                pet_id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL,
                pet_type VARCHAR NOT NULL,
                birth_date DATE NOT NULL,
                OWNER VARCHAR NOT NULL);
              """,
    )
