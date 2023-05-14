from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


from twitter_etl import run_twitter_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['test@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}



dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='My firt etl code'
)


run_etl = PythonOperator(
    task_id='ketul_twitter_etl',
    python_callable=run_twitter_etl,
    dag=dag, 
)

run_etl
