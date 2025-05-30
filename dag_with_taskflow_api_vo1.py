from airflow.decorators import dag, task
from datetime import datetime,timedelta


default_args={
    'owner':'binisha',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

@dag(dag_id='dag_with_taskflow_api_vo1',
     default_args=default_args,
     start_date=datetime(2025,5,28),
     schedule_interval='@daily')
def hello_world_etl():
    


    @task()
    def get_name():
        return "Sita"
    
    @task()
    def get_age():
        return 23
    
    @task()
    def greet(name: str, age: int):
        print(f"Hello World! My name is {name} and I am {age} years old!")

    name = get_name()
    age = get_age()
    greet(name=name, age=age)

# Instantiate the DAG
hello_world_etl()



        








