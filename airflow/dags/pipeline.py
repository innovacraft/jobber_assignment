from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Importing my processing scripts
from scripts.ingestion import run_ingestion
from scripts.processing import run_processing
from scripts.storage import run_storage
from scripts.consumption import run_consumption

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'jobber_assignment_pipeline',
    default_args=default_args,
    description='A DAG for the Jobber Assignment Data Pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 2, 7),
    catchup=False,
)

# Define tasks from project
t1 = PythonOperator(
    task_id='ingestion',
    python_callable=run_ingestion,
    dag=dag,
)

t2 = PythonOperator(
    task_id='processing',
    python_callable=run_processing,
    dag=dag,
)

t3 = PythonOperator(
    task_id='storage',
    python_callable=run_storage,
    dag=dag,
)

t4 = PythonOperator(
    task_id='consumption',
    python_callable=run_consumption,
    dag=dag,
)

# Set task dependencies
t1 >> t2 >> t3 >> t4


