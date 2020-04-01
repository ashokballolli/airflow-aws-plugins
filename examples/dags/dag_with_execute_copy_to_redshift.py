from airflow import DAG
from aws_operators.operators.redshift_operators import ExecuteCopyToRedshiftOperator
from datetime import datetime


with DAG('dag_with_execute_copy_to_redshift_operator', start_date=datetime(2018, 8, 11)) as dag:
    (
        ExecuteCopyToRedshiftOperator(
            task_id='redshift_copy_append',
            redshift_conn_id='redshift_dev',
            s3_bucket='it-metrics-dicts-dw-stst',
            s3_key='finance/costs.csv',
            redshift_schema='public',
            redshift_table='test_costs',
            iam_role='arn:aws:iam::664994689501:role/ItMetricsCopyRedshiftExecutionRole',
            mode='append',
            copy_params=["delimiter ';'", "ignoreheader as 1"]
        )
        >> ExecuteCopyToRedshiftOperator(
            task_id='redshift_copy_overwrite',
            redshift_conn_id='redshift_dev',
            s3_bucket='it-metrics-dicts-dw-stst',
            s3_key=lambda c: "year={}/month={}/epics.json".format(c['execution_date'].year, c['execution_date'].strftime('%m')),
            redshift_schema='public',
            redshift_table='test_epics',
            iam_role='arn:aws:iam::664994689501:role/ItMetricsCopyRedshiftExecutionRole',
            mode='overwrite',
            copy_params=["json 'auto'"]
        )
        >> ExecuteCopyToRedshiftOperator(
            task_id='redshift_copy_append_overwrite',
            redshift_conn_id='redshift_dev',
            s3_bucket='it-metrics-presentation-dw-stst',
            s3_key=lambda c: "timesheet/year={}/month={}/day={}".format(c['execution_date'].year, c['execution_date'].strftime('%m'), c['execution_date'].strftime('%d')),
            redshift_schema='public',
            redshift_table='test_worklogs',
            iam_role='arn:aws:iam::664994689501:role/ItMetricsCopyRedshiftExecutionRole',
            mode='append_overwrite',
            where_condition_fn=lambda c: "date_activity = '{}'".format(c['execution_date'].strftime('%Y-%m-%d')),
            copy_params=["json 'auto'"]
        )
    )
