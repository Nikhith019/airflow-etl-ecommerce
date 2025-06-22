from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.etl_pipeline import extract, transform, load

default_args = {
    'owner': 'nikhith',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecom_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for e-commerce sales data',
    schedule_interval=None,
    start_date=datetime(2025, 6, 21),
    catchup=False,
    tags=['ecommerce', 'etl']
) as dag:

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    task_extract >> task_transform >> task_load
