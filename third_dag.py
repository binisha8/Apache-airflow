from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args={
    'owner': 'airflow',
    'retries':5,
    'retry_delay':timedelta(minutes=2)

}

with DAG(
    dag_id="first_dag_v3",
    default_args=default_args,
    description='this is our first dag that we write',
    start_date=datetime(2025,5,26,2),
    schedule_interval='@daily',
    catchup=False
    
)as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command="echo hello world,this is the first task!"
    )
    task2=BashOperator(
        task_id='second_task',
        bash_command="echo hello world,this is the second task!"
    )
    task3=BashOperator(
        task_id='third_task',
        bash_command="echo hello world,this is the third task!"
    )
       #first method
    #task1.set_downstream(task2)
    #task1.set_downstream(task3)
       #second method
    #task1>>task2
    #task1>>task3
       #third method
    task1>>[task2,task3]

