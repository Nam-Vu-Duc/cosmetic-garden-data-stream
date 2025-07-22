import sys
sys.path.append('/opt/airflow')
from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from scripts.fact_stream_data import stream_fact_tables
from scripts.dim_stream_data import stream_dim_tables
from scripts.detect_fraud import detect_fraud

default_args = {
    'owner': 'namvu',
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}

with (DAG(
    default_args=default_args,
    dag_id='dag_for_jobs_market_etl',
    description='dag_for_jobs_market_etl',
    start_date=datetime(2025, 4, 16),
    # schedule_interval='@daily'
) as dag):

    stream_fact_tables = PythonOperator(
        task_id='initial_requirements',
        python_callable=stream_fact_tables,
    )

    stream_dim_tables = PythonOperator(
        task_id='initial_requirements',
        python_callable=stream_dim_tables,
    )

    detect_fraud = PythonOperator(
        task_id='initial_requirements',
        python_callable=detect_fraud,
    )