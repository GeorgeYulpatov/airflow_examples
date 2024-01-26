from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'GeorgeYA',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag_v5',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2023, 10, 30, 2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo Я первая задача!'
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo Я вторая задача, работаю после выполнения первой задачи!'
    )

    task3 = BashOperator(
        task_id='thrid_task',
        bash_command='echo Я третья задача, работаю после выполнения первой задачи одновременно со второй!'
    )

    # Метод установки зависимости задач №1
    # (задача 2 и 3 выполняются одновременно после окончания задачи 1)

    #task1.set_downstream(task2)
    #task1.set_downstream(task3)

    # Метод установки зависимости задач №2
    # (задача 2 и 3 выполняются одновременно после окончания задачи 1)

    #task1 >> task2
    #task1 >> task3

    # Метод установки зависимости задач №3
    # (задача 2 и 3 выполняются одновременно после окончания задачи 1)

    task1 >>[task2, task3]

