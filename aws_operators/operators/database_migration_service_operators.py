from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils import apply_defaults
import logging
import boto3
import time


class StartDMSReplicationTaskOperator(BaseOperator):

    ui_color = '#ff9900'

    @apply_defaults
    def __init__(
            self,
            replication_task_arn,
            start_replication_task_type,
            polling_interval,
            *args,
            **kwargs
    ):
        """
        Trigger AWS Glue Job function

        :param job_name: the name of the Glue job to start and monitor
        :param polling_interval: time interval, in seconds, to check the status of the job
        """
        super(StartDMSReplicationTaskOperator, self).__init__(*args, **kwargs)
        self.replication_task_arn = replication_task_arn
        self.start_replication_task_type = start_replication_task_type
        self.polling_interval = polling_interval
        self.dms_client = boto3.client('dms')

    def execute(self, context):
        start_replication_task_response = self.dms_client.start_replication_task(
            ReplicationTaskArn=self.replication_task_arn, StartReplicationTaskType=self.start_replication_task_type)
        logging.info(start_replication_task_response)

        logging.info("start_replication_task_response: " +
              str(start_replication_task_response) + "\n")
        replication_instance_arn = start_replication_task_response.get(
            "ReplicationTask", {}).get("ReplicationInstanceArn", {})
        logging.info("replication_instance_arn: " +
              str(replication_instance_arn) + "\n")
        if start_replication_task_response.get("ReplicationTask", {}).get("Status", {}) != "starting":
            logging.error("Failed to Start the Replication Task\n")
            raise AirflowException('Failed to Start the Replication Task')
        while True:
            describe_replication_tasks_response = self.dms_client.describe_replication_tasks(
                Filters=[{"Name": "replication-instance-arn", "Values": [replication_instance_arn]}])
            logging.info("describe_replication_tasks_response: " +
                str(describe_replication_tasks_response) + "\n")
            replication_task_status = describe_replication_tasks_response.get(
                "ReplicationTasks", [{}])[0].get("Status", None)
            logging.info("Current Status: " + str(replication_task_status) + "\n")
            if replication_task_status in (None, "failed"):
                logging.error("The Replication Task Failed\n")
                raise AirflowException('The Replication Task Failed')
            if replication_task_status in ("success"):
                logging.info("The Replication Task Succeeded\n")
                break
            logging.info("Sleeping for " + str(self.polling_interval) + " seconds...\n")
            time.sleep(self.polling_interval)
