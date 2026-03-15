from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args={
'owner':'S260100',
'retries': 1,
'retry_delay': timedelta(minutes=1)
}


dag=DAG(
dag_id='S260100',
start_date=datetime(2025,1,31),
schedule='0 15 * * *',
description='ETL',
default_args=default_args,
catchup=False
)

ENV=Variable.get('ENV',default_var='dev')
task=BashOperator(
task_id='Trigger',
dag=dag,
bash_command=f'bash /opt/spark-apps/S260100/shell/run.sh {ENV}'
)
