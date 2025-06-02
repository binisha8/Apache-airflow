from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from datacleaner import data_cleaner


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'data_clean',
    default_args=default_args,
    schedule_interval='@daily' ,
    catchup=False
)

t1 = BashOperator(
    task_id='check_file_exists',
    bash_command='shasum /home/binisha/airflow/store_files/raw_store_transactions.csv',
    retries=2,
    retry_delay=timedelta(seconds=15),
    dag=dag
)

t2 = PythonOperator(
    task_id='check_raw_csv',
    python_callable= data_cleaner,
    dag=dag
)

t1 >> t2
