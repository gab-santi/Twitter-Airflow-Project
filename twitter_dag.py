from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from twitter_etl import run_twitter_etl
import pendulum

# initalize args
default_args = {
    'owner': 'airflow',
    'depends_on_post': False,
    'start_date': pendulum.yesterday(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_interval': '0 0 * * *'
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
 
