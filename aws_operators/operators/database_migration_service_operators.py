from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils import apply_defaults
import boto3
import humps
import logging
import time


class StartDMSReplicationTaskOperator(BaseOperator):

    ui_color = '#ff9900'

    @apply_defaults
    def __init__(
            self,
            replication_task_arn,
            start_replication_task_type,
            polling_interval=10,
            *args,
            **kwargs
    ):
        """
        Trigger AWS Data Migration Services Replication Task function

        :param replication_task_arn (string) [REQUIRED] -- The Amazon Resource Name (ARN) of the replication task to be started
        :param start_replication_task_type (string) [REQUIRED] -- The type of replication task. Possible Values include start-replication, resume-processing, reload-target
        :param polling_interval (integer) (default: 10) -- time interval, in seconds, to check the status of the job
        :param cdc_start_time (datetime) -- Indicates the start time for a change data capture (CDC) operation. Use either CdcStartTime or CdcStartPosition to specify when you want a CDC operation to start. Specifying both values results in an error.
        :param cdc_start_position (string) -- Indicates when you want a change data capture (CDC) operation to start. Use either CdcStartPosition or CdcStartTime to specify when you want a CDC operation to start. Specifying both values results in an error.
        :param cdc_stop_position (string) -- Indicates when you want a change data capture (CDC) operation to stop. The value can be either server time or commit time.

        Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dms.html#DatabaseMigrationService.Client.start_replication_task
        """
        super(StartDMSReplicationTaskOperator, self).__init__(*args, **kwargs)
        self.replication_task_arn = replication_task_arn
        self.start_replication_task_type = start_replication_task_type
        self.polling_interval = polling_interval
        self.dms_client = boto3.client('dms')

        self.pascalized_args = {humps.pascalize(
            k): v for k, v in self.__dict__.items()}
        boto3_dms_arguments = ['ReplicationTaskArn', 'StartReplicationTaskType',
                               'CdcStartTime', 'CdcStartPosition', 'CdcStopPosition']
        self.func_args = {key: self.pascalized_args[key] for key in set(
            boto3_dms_arguments).intersection(self.pascalized_args.keys())}

    def execute(self, context):
        start_replication_task_response = self.dms_client.start_replication_task(
            ReplicationTaskArn=self.replication_task_arn, StartReplicationTaskType=self.start_replication_task_type)
        replication_task_arn = start_replication_task_response.get(
            "ReplicationTask", {}).get("ReplicationTaskArn", {})
        logging.info("replication_task_arn: " +
                     str(replication_task_arn) + "\n")
        if start_replication_task_response.get("ReplicationTask", {}).get("Status", {}) != "starting":
            logging.error("Failed to Start the Replication Task\n")
            raise AirflowException('Failed to Start the Replication Task')
        while True:
            describe_replication_tasks_response = self.dms_client.describe_replication_tasks(
                Filters=[{"Name": "replication-task-arn", "Values": [replication_task_arn]}])
            replication_task_status = describe_replication_tasks_response.get(
                "ReplicationTasks", [{}])[-1].get("Status", None)
            replication_task_progress = describe_replication_tasks_response.get(
                "ReplicationTasks", [{}])[-1].get("ReplicationTaskStats", {}).get("FullLoadProgressPercent", None)
            logging.info("Current Status: " +
                         str(replication_task_status) + "\n")
            logging.info("Current Load Progress: " +
                         str(replication_task_progress) + "\n")
            if replication_task_status in (None, "failed"):
                logging.error("The Replication Task Failed\n")
                raise AirflowException('The Replication Task Failed')
            elif replication_task_status in ("stopped"):
                replication_task_stats = describe_replication_tasks_response.get(
                    "ReplicationTasks", [{}])[-1].get("ReplicationTaskStats", {})
                logging.info("Replication Task Stats: " +
                             str(replication_task_stats))
                tables_errored = describe_replication_tasks_response.get(
                    "ReplicationTasks", [{}])[-1].get("ReplicationTaskStats", {}).get("TablesErrored", None)
                if (tables_errored != 0):
                    logging.error("Count of Errored Tables: " +
                                  str(tables_errored))
                    raise AirflowException(
                        "The Replication Task DID NOT FULLY succeed.")
                logging.info("The Replication Task Succeeded\n")
                break
            else:
                logging.info("Sleeping for " +
                             str(self.polling_interval) + " seconds...\n")
                time.sleep(self.polling_interval)
