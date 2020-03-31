from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from aws_operators.operators.database_migration_service_operators import StartDMSReplicationTaskOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'dag_with_dms_operators',
    default_args=default_args,
    description='Sample DAG with AWS Data Migration Service (DMS) operators',
    schedule_interval=None,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

dms_replication = StartDMSReplicationTaskOperator(
    task_id='dms_replication',
    replication_task_arn='dms-airflow',
    start_replication_task_type='start-replication', # Possible Values --> 'start-replication'|'resume-processing'|'reload-target'
    polling_interval=10,
    dag=dag
)

start >> dms_replication
