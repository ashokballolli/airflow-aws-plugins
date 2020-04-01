from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from aws_operators.operators._lambda_operators import ExecuteLambdaOperator
from datetime import datetime


with DAG('dag_with_execute_lambda_operator', start_date=datetime(2018, 8, 11)) as dag:
    (
        BashOperator(
            task_id='bash_hello',
            bash_command='echo "HELLO!"'
        )
        >> ExecuteLambdaOperator(
            task_id='task_with_execute_lambda_operator',
            airflow_context_to_lambda_payload=lambda c: {
                "date": c["execution_date"].strftime('%Y-%m-%d')},
            additional_payload={"ap1": "c1", "ap2": 21},
            lambda_function_name="ItMetricsCalculateSharedAllocationKeysFunction"
        )
    )
