import sys
sys.path.append('/opt/airflow')
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.fact import streaming_data, storing_data_lake, storing_data_warehouse
from scripts.dim import streaming_data, insert_new_record, de_active_current_record, storing_data_warehouse
from scripts.fraud import detecting_fraud, send_email

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

    storing_data_lake = PythonOperator(
        task_id='initial_requirements',
        python_callable=storing_data_lake,
    )

    storing_data_warehouse = PythonOperator(
        task_id='initial_requirements',
        python_callable=storing_data_warehouse,
    )

    detecting_fraud = PythonOperator(
        task_id='initial_requirements',
        python_callable=detecting_fraud,
    )

    stream1 = storing_data_lake
    stream2 = detecting_fraud