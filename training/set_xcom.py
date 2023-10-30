from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from random import uniform
from datetime import datetime


def set_variable(ti):
    print(f"we are going to set Spark URL")
    ti.xcom_push(key="SPARK_URL",
                 value="spark://spark-1698694077-master-0.spark-1698694077-headless.default.svc.ai.rasha.local:7077")


def get_variable(ti):
    print(f"this is value of SPARK_URL:{ti.xcom_pull(key='SPARK_URL')}")


with DAG('xcom_dag',
         start_date=datetime(2023, 10, 30),
         schedule='@daily',
         catchup=False):
    set_spark_url = PythonOperator(
        task_id=f'set_spark_url',
        python_callable=set_variable,
        do_xcom_push=True
    )
    get_spark_url = PythonOperator(
        task_id="get_spark_url",
        python_callable=get_variable,
    )
    set_spark_url >> get_spark_url
