from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from aws_operators.operators.glue_operators import StartGlueJobRunOperator, \
    StartGlueWorkflowRunOperator, StartGlueCrawlerOperator, StartGlueTriggerOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG('dag_with_glue_operators', default_args=default_args, description='Sample DAG with AWS Glue operators', schedule_interval=None) as dag:

    start = DummyOperator(
        task_id='start',
    )

    glue_job_operator = StartGlueJobRunOperator(
        task_id='glue_job_operator',
        job_name='airflow',
        polling_interval=10,
    )

    glue_workflow_operator = StartGlueWorkflowRunOperator(
        task_id='glue_workflow_operator',
        workflow_name='airflow',
        polling_interval=10,
    )

    glue_crawler_operator = StartGlueCrawlerOperator(
        task_id='glue_crawler_operator',
        crawler_name='airflow',
        polling_interval=10,
    )

    glue_trigger_operator = StartGlueTriggerOperator(
        task_id='glue_trigger_operator',
        trigger_name='airflow',
    )

    start >> [glue_job_operator, glue_workflow_operator,
              glue_crawler_operator, glue_trigger_operator]
