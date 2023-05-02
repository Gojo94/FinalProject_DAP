from datetime import datetime
from airflow.models import DAG,Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator


default_args={
    "owner":"Emeka",
    "start_date":datetime(2023,5,1)
}

dag = DAG(
    'Non_Motorist_Crashes',
    default_args=default_args,
    schedule_interval=None
)

automation_task = BashOperator(
    task_id='Extract',
    bash_command='python /opt/airflow/dags/bots/GetDataFromMongo.py',
    dag=dag
)


automation_task_2 = BashOperator(
    task_id='Transform',
    bash_command='python /opt/airflow/dags/bots/Transform_Data.py',
    dag=dag
)

automation_task_3 = BashOperator(
    task_id='Load',
    bash_command='python /opt/airflow/dags/bots/Load_to_Postgres.py',
    dag=dag
)

automation_task >> automation_task_2 >> automation_task_3