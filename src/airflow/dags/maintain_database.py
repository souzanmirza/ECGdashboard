from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'me',
    'start_date': datetime.now(),
    'max_active_runs':1,
}


with DAG('maintain_database',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),
         catchup=False) as dag:
    do_tasks = BashOperator(task_id='do_tasks',
                                 bash_comand='python dump_delete_database.py')