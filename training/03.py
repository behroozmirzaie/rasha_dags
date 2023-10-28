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
    'conn_id': 'postgres_data_source_1'
}

with DAG(
        default_args=default_args,
        dag_id="postgres_operator_dag",
        start_date=dt.datetime.now(),
        schedule_interval="@daily",

) as dag:
    create_pet_table = PostgresOperator(
        postgres_conn_id="postgres_data_source_1",
        task_id="create_pet_table",
        sql="""
                CREATE TABLE IF NOT EXISTS FATEMEH (
                id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL,
                last_name VARCHAR NOT NULL,
                birth_date DATE NOT NULL);
              """,
    )

    insert_to_table = PostgresOperator(
        postgres_conn_id="postgres_data_source_1",
        task_id="insert_to_table",
        sql="""
        INSERT INTO FATEMEH (name, last_name, birth_date) VALUES
                ('FatemEh Azimi', 'Hashemi', '2003-03-08'),
                ('Sara Hosseini', 'Farahani', '2002-04-09'),
                ('Ali Mohammadi', 'Ahmadabadi', '2001-05-10'),
                ('Kimia Ahmadi', 'Safaie', '2000-06-11'),
                ('Mohammadreza Jafari', 'Salehi', '1999-07-12'),
                ('Mahnaz Alizadeh', 'Hosseini', '1998-08-13'),
                ('Hossein Ahmadi', 'Mohammadi', '1997-09-14'),
                ('Zahra Hashemi', 'Jafari', '1996-10-15'),
                ('Alireza Mohammadi', 'Salehi', '1995-11-16'),
                ('Fatemeh Hosseini', 'Alizadeh', '1994-12-17');
                """,
    )

    create_pet_table >> insert_to_table
