from airflow.plugins_manager import AirflowPlugin
from aws_operators.operators.redshift_operators import ExecuteRedshiftQueryOperator, ExecuteCopyToRedshiftOperator
from aws_operators.operators._lambda_operators import ExecuteLambdaOperator
from aws_operators.operators.glue_operators import StartGlueJobRunOperator, \
    StartGlueWorkflowRunOperator, StartGlueCrawlerOperator, StartGlueTriggerOperator
from aws_operators.operators.database_migration_service_operators import StartDMSReplicationTaskOperator


class AWSOperatorsPlugin(AirflowPlugin):
    name = "AWS operators plugin"
    operators = [
        ExecuteRedshiftQueryOperator,
        ExecuteCopyToRedshiftOperator,
        ExecuteLambdaOperator,
        StartGlueJobRunOperator,
        StartGlueWorkflowRunOperator,
        StartGlueCrawlerOperator,
        StartGlueTriggerOperator,
        StartDMSReplicationTaskOperator
    ]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
