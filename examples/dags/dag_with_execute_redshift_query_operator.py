from airflow import DAG
from aws_operators.operators.redshift_operators import ExecuteRedshiftQueryOperator
from datetime import datetime


with DAG('dag_with_execute_redshift_operator', start_date=datetime(2018, 8, 11)) as dag:
    (
        ExecuteRedshiftQueryOperator(
            task_id='drop_table',
            redshift_conn_id='redshift_dev',
            query='DROP TABLE IF EXISTS TEST_TABLE'
        )
        >> ExecuteRedshiftQueryOperator(
            task_id='create_table',
            redshift_conn_id='redshift_dev',
            query='CREATE TABLE TEST_TABLE AS SELECT current_date'
        )
    )
