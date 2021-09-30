from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from spotify_etl import spotify_etl_func

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date' : datetime.now(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id="spotify_etl",
    default_args=default_args,
    description='Used to automate ETL of spotify data on a 24hr cycle',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='spotify_etl',
    python_callable=spotify_etl_func,
    dag=dag
)

run_etl