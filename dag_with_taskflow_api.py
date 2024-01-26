from datetime import datetime, timedelta

from airflow.decorators import dag, task


default_args = {
    'owner': 'GeorgeYA',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id= 'dag_with_taskflow_api_v02',
     default_args= default_args,
     start_date=datetime(2023, 10, 30, 2),
     schedule_interval='@daily')
def hello_world_et():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Jerry',
            'last_name': 'Fridman'
        }
    
    @task()
    def get_age():
        return "19"

    @task()
    def greet(first_name, last_name, age):
        print(f"Привет, меня зовут {first_name} {last_name}. Мой возраст {age} лет")
        
    name_dict= get_name()
    age = get_age()
    greet(first_name= name_dict['first_name'],
          last_name= name_dict['last_name'],
          age=age)

greet_dag = hello_world_et()
