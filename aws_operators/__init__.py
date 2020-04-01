from airflow.plugins_manager import AirflowPlugin
from aws_operators.operators.redshift_operators import ExecuteRedshiftQueryOperator, ExecuteCopyToRedshiftOperator
from aws_operators.operators._lambda_operators import ExecuteLambdaOperator


class AWSOperatorsPlugin(AirflowPlugin):
    name = "AWS operators plugin"
    operators = [
        ExecuteRedshiftQueryOperator,
        ExecuteCopyToRedshiftOperator,
        ExecuteLambdaOperator
    ]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
