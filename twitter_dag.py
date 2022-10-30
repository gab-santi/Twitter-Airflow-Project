from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from twitter_etl import run_twitter_etl

# initalize args
default_args = {
    'owner': 'airflow',
    'depends_on_post': False,
    'start_date': datetime(2022,11,8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# initialize DAG
dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='Sample ETL code'
)

# initialize operators
run_etl = PythonOperator(
    task_id='complete_twitter_etl',
    python_callable=run_twitter_etl,
    dag=dag
)
 
