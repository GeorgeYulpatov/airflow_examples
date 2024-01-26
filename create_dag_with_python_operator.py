from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'GeorgeYA',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def greeting (ti):
    first_name = ti.xcom_pull(task_ids= 'get_name', key= 'first_name')
    last_name = ti.xcom_pull(task_ids= 'get_name', key= 'last_name')
    age = ti.xcom_pull(task_ids= 'get_age', key= 'age')
    print(f'Привет, меня зовут {first_name} {last_name}, мне {age} года!')


def get_name(ti):
    ti.xcom_push(key= 'first_name', value= 'George')
    ti.xcom_push(key= 'last_name', value= 'Yulpatov')


def get_age(ti):
    ti.xcom_push(key= 'age', value= '23')

with DAG(
    dag_id='our_dag_with_python_operator_v05',
    default_args=default_args,
    description='Этот DAG написан с использованием Python операторов',
    start_date=datetime(2023, 10, 30, 2),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id= 'greeting',
        python_callable= greeting,
        #op_kwargs={'age': '23'}
    )

    task2 = PythonOperator(
        task_id= 'get_name',
        python_callable= get_name,
    )

    task3 = PythonOperator(
        task_id= 'get_age',
        python_callable= get_age,
    )

    [task2, task3] >> task1
