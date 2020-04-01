from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException
from botocore.config import Config

import boto3
import json
import logging
import base64


class ExecuteLambdaOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            lambda_function_name,
            airflow_context_to_lambda_payload=None,
            additional_payload={},
            *args,
            **kwargs
    ):
        """
        Trigger AWS Lambda function

        :param airflow_context_to_lambda_payload: function extracting fields from Airflow context to Lambda payload
        :param additional_payload: additional parameters for Lambda payload
        :param lambda_function_name: name of Lambda function
        """
        super(ExecuteLambdaOperator, self).__init__(*args, **kwargs)
        self.airflow_context_to_lambda_payload = airflow_context_to_lambda_payload
        self.additional_payload = additional_payload
        self.lambda_function_name = lambda_function_name
        self.lambda_client = boto3.client('lambda', config=Config(read_timeout=300, connect_timeout=300))

    def execute(self, context):
        request_payload = self.__create_lambda_payload(context)

        logging.info('Executing AWS Lambda {} with payload {}'.format(self.lambda_function_name, request_payload))

        response = self.lambda_client.invoke(
            FunctionName=self.lambda_function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(request_payload),
            LogType='Tail'
        )

        response_log_tail = base64.b64decode(response.get('LogResult'))
        response_payload = json.loads(response.get('Payload').read())
        response_code = response.get('StatusCode')

        log_msg_logs = 'Tail of logs from AWS Lambda:\n{logs}'.format(logs=response_log_tail)
        log_msg_payload = 'Response payload from AWS Lambda:\n{resp}'.format(resp=response_payload)

        if response_code == 200:
            logging.info(log_msg_logs)
            logging.info(log_msg_payload)
            return response_code
        else:
            logging.error(log_msg_logs)
            logging.error(log_msg_payload)
            raise AirflowException('Lambda invoke failed')

    def __create_lambda_payload(self, context):
        payload = self.airflow_context_to_lambda_payload(context) if self.airflow_context_to_lambda_payload is not None else {}
        payload.update(self.additional_payload)
        return payload
