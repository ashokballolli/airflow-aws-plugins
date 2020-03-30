from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from aws_operators.operators.glue_operators import StartGlueJobRunOperator, StartGlueWorkflowRunOperator, StartGlueCrawlerRunOperator
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
    'dag_with_glue_operators.py',
    default_args=default_args,
    description='Sample DAG with AWS Glue operators',
    schedule_interval=None,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

glue_job = StartGlueJobRunOperator(
    task_id='glue-job',
    job_name='glue-airflow',
    polling_interval=10,
    dag=dag
)

glue_workflow = StartGlueWorkflowRunOperator(
    task_id='glue-workflow',
    workflow_name='glue-workflow',
    polling_interval=10,
    dag=dag
)

glue_crawler = StartGlueCrawlerRunOperator(
    task_id='glue-crawler',
    crawler_name='pjain-sacramento',
    polling_interval=10,
    dag=dag
)
    
start >> glue_job
start >> glue_workflow
start >> glue_crawler