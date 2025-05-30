from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator



default_args={
    'owner':'airflow',
    'retries': 5,
    'retry_delay':timedelta(minutes=5)
    
}

def greet():
    print("Hello World! My name is {name}, "
          f"and I am {age} years old!")
    
def get_name():
    return 'Maharjan'


with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v01',
    description='Our first dag using pythoon operator',
    start_date = datetime(2025,5,27),
    schedule_interval='@daily'
) as dag:
    task1=PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'name':'Binisha','age':23}
    )

    task1