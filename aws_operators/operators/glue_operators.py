from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils import apply_defaults
import boto3
import humps
import logging
import time


class StartGlueJobRunOperator(BaseOperator):

    ui_color = '#ff9900'

    @apply_defaults
    def __init__(
            self,
            job_name,
            polling_interval=10,
            *args,
            **kwargs
    ):
        """
        Trigger AWS Glue Job function

        :param job_name (string) [REQUIRED] -- the name of the Glue job to start and monitor
        :param polling_interval (integer) (default: 10) -- time interval, in seconds, to check the status of the job
        :param job_run_id (string) -- The ID of a previous JobRun to retry.
        :param arguments (dict) -- The job arguments specifically for this run. For this job run, they replace the default arguments set in the job definition itself.
        :param timeout (integer) -- The JobRun timeout in minutes.
        :param max_capacity (float) -- The number of AWS Glue data processing units (DPUs) that can be allocated when this job runs.
        :param security_configuration (string) -- The name of the SecurityConfiguration structure to be used with this job run.
        :param notification_property (dict) -- Specifies configuration properties of a job run notification.
        :param worker_type (string) -- The type of predefined worker that is allocated when a job runs. Accepts a value of Standard, G.1X, or G.2X.
        :param number_of_workers (integer) -- The number of workers of a defined workerType that are allocated when a job runs.

        Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.start_job_run
        """
        super(StartGlueJobRunOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.polling_interval = polling_interval
        self.__dict__.update(kwargs)

        self.glue_client = boto3.client('glue')

        self.pascalized_args = {humps.pascalize(
            k): v for k, v in self.__dict__.items()}
        boto3_glue_arguments = ['JobRunId', 'Arguments', 'Timeout', 'MaxCapacity',
                                'SecurityConfiguration', 'NotificationProperty', 'WorkerType', 'NumberOfWorkers']
        self.func_args = {key: self.pascalized_args[key] for key in set(
            boto3_glue_arguments).intersection(self.pascalized_args.keys())}

    def execute(self, context):

        logging.info(self.func_args)
        start_glue_job_response = self.glue_client.start_job_run(
            **self.func_args)
        logging.info(start_glue_job_response)

        glue_job_id = start_glue_job_response['JobRunId']
        logging.info("Glue Job ID: " + str(glue_job_id))

        while True:
            job_status = self.glue_client.get_job_run(
                JobName=self.job_name, RunId=glue_job_id)['JobRun']['JobRunState']
            logging.info("Job Status: " + str(job_status))

            # Possible values --> 'JobRunState': 'STARTING'|'RUNNING'|'STOPPING'|'STOPPED'|'SUCCEEDED'|'FAILED'|'TIMEOUT'
            if (job_status in ['STARTING', 'RUNNING']):
                logging.info("Sleeping for " +
                             str(self.polling_interval) + " seconds...\n")
                time.sleep(self.polling_interval)
            elif (job_status in ['STOPPING', 'STOPPED', 'FAILED', 'TIMEOUT']):
                logging.error(
                    "Message: " + str(job_status.get("JobRun").get("ErrorMessage", "No Error Message Present")))
                logging.error("Something went wrong. Check AWS Logs. Exiting.")
                raise AirflowException('AWS Glue Job Run Failed')
            else:
                break


class StartGlueWorkflowRunOperator(BaseOperator):

    ui_color = '#ff9900'

    @apply_defaults
    def __init__(
            self,
            workflow_name,
            polling_interval=10,
            *args,
            **kwargs
    ):
        """
        Trigger AWS Glue Workflow function

        :param workflow_name (string) [REQUIRED]: the name of the Glue workflow to start and monitor
        :param polling_interval (integer) (default: 10) -- time interval, in seconds, to check the status of the job
        """
        super(StartGlueWorkflowRunOperator, self).__init__(*args, **kwargs)
        self.workflow_name = workflow_name
        self.polling_interval = polling_interval
        self.glue_client = boto3.client('glue')

    def execute(self, context):
        start_glue_workflow_response = self.glue_client.start_workflow_run(
            Name=self.workflow_name)
        logging.info(start_glue_workflow_response)

        glue_workflow_id = start_glue_workflow_response['RunId']
        logging.info("Glue Workflow ID: " + str(glue_workflow_id))

        while True:
            workflow_status = self.glue_client.get_workflow_run(
                Name=self.workflow_name, RunId=glue_workflow_id)['Run']['Status']
            if (workflow_status != "COMPLETED"):
                logging.info("Workflow Status: " + str(workflow_status))
                logging.info("Workflow Stats: " +
                             str(workflow_status['Run']['Statistics']))
                logging.info("Sleeping for " +
                             str(self.polling_interval) + " seconds...\n")
                time.sleep(self.polling_interval)
            else:
                total_actions = workflow_status['Run']['Statistics']['TotalActions']
                succeeded_actions = workflow_status['Run']['Statistics']['SucceededActions']

                if (succeeded_actions != total_actions):
                    logging.error("All actions did NOT succeed.")
                    for entry in (workflow_status['Run']['Graph']['Nodes']):
                        output = entry['Type'] + ": " + entry['Name'] + " --> "
                        if (entry['Type'] == "JOB"):
                            try:
                                output += entry['JobDetails']['JobRuns'][-1]['JobRunState']
                            except KeyError as e:
                                output += "Job state not available"
                            print(output)
                        if (entry['Type'] == "CRAWLER"):
                            try:
                                output += entry['CrawlerDetails']['Crawls'][-1]['State']
                            except KeyError as e:
                                output += "Crawler state not available"
                            print(output)
                    logging.error("Check AWS Logs. Exiting.")
                    raise AirflowException('AWS Glue Workflow Run Failed')
                else:
                    logging.info("All actions succeeded.")
                    break


class StartGlueCrawlerOperator(BaseOperator):

    ui_color = '#ff9900'

    @apply_defaults
    def __init__(
            self,
            crawler_name,
            polling_interval=10,
            *args,
            **kwargs
    ):
        """
        Trigger AWS Glue Crawler function

        :param crawler_name (string) [REQUIRED]: the name of the Glue crawler to start and monitor
        :param polling_interval (integer) (default: 10) -- time interval, in seconds, to check the status of the job
        """
        super(StartGlueCrawlerOperator, self).__init__(*args, **kwargs)
        self.crawler_name = crawler_name
        self.polling_interval = polling_interval
        self.glue_client = boto3.client('glue')

    def execute(self, context):

        # Retrieving last_crawl details to compare with later
        last_crawl_before_starting = self.glue_client.get_crawler(
            Name=self.crawler_name)['Crawler']['LastCrawl']

        start_glue_crawler_response = self.glue_client.start_crawler(
            Name=self.crawler_name)
        logging.info(start_glue_crawler_response)

        while True:
            crawler_status = self.glue_client.get_crawler(
                Name=self.crawler_name)['Crawler']['State']
            logging.info("Crawler Status: " + str(crawler_status))

            # Possible values --> 'State': 'READY'|'RUNNING'|'STOPPING'
            if (crawler_status in ['RUNNING']):
                logging.info("Sleeping for " +
                             str(self.polling_interval) + " seconds...\n")
                time.sleep(self.polling_interval)
            elif (crawler_status in ['STOPPING', 'READY']):
                last_crawl_at_stopping = self.glue_client.get_crawler(Name=self.crawler_name)[
                    'Crawler']['LastCrawl']
                if (last_crawl_before_starting == last_crawl_at_stopping):
                    logging.info("Sleeping for " +
                                 str(self.polling_interval) + " seconds...\n")
                    time.sleep(self.polling_interval)
                else:
                    final_response = self.glue_client.get_crawler(
                        Name=self.crawler_name)
                    final_status = final_response['Crawler']['LastCrawl']['Status']

                    # Possible values --> 'Status': 'SUCCEEDED'|'CANCELLED'|'FAILED'
                    if (final_status in ['SUCCEEDED']):
                        logging.info("Final Crawler Status: " +
                                     str(final_status))
                        break
                    else:
                        logging.error(
                            "Final Crawler Status: " + str(final_status))
                        logging.error("Message: " + str(final_response.get("Crawler").get(
                            "LastCrawl").get("ErrorMessage", "No Error Message Present")))
                        logging.error(
                            "Check AWS Logs. Exiting.")
                        raise AirflowException('AWS Crawler Job Run Failed')


class StartGlueTriggerOperator(BaseOperator):

    ui_color = '#ff9900'

    @apply_defaults
    def __init__(
            self,
            trigger_name,
            *args,
            **kwargs
    ):
        """
        Trigger AWS Glue Trigger function

        :param trigger_name (string) [REQUIRED]: the name of the Glue trigger to start
        """
        super(StartGlueTriggerOperator, self).__init__(*args, **kwargs)
        self.trigger_name = trigger_name
        self.glue_client = boto3.client('glue')

    def execute(self, context):
        start_glue_trigger_response = self.glue_client.start_trigger(
            Name=self.trigger_name)
        logging.info(start_glue_trigger_response)
