import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
        dag_id="first_dag_training",
        start_date=datetime.datetime(2023, 1, 1),
        schedule="@daily",
):
    EmptyOperator(task_id="task")

