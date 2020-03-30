from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils import apply_defaults
import logging
import boto3
import time
import sys

class StartGlueJobRunOperator(BaseOperator):

    ui_color = '#ff9900'

    @apply_defaults
    def __init__(
            self,
            job_name,
            polling_interval,
            *args,
            **kwargs
    ):
        """
        Trigger AWS Glue Job function

        :param job_name: the name of the Glue job to start and monitor
        :param polling_interval: time interval, in seconds, to check the status of the job
        """
        super(StartGlueJobRunOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.polling_interval = polling_interval
        self.glue_client = boto3.client('glue')

    def execute(self, context):
        start_glue_job_response = self.glue_client.start_job_run(JobName=self.job_name)
        logging.info(start_glue_job_response)

        glue_job_id = start_glue_job_response['JobRunId']
        logging.info("Glue Job ID: " + str(glue_job_id))

        while True:
            job_status = self.glue_client.get_job_run(JobName=self.job_name,RunId=glue_job_id)['JobRun']['JobRunState']
            logging.info("Job Status: " + str(job_status))

            # Possible values --> 'JobRunState': 'STARTING'|'RUNNING'|'STOPPING'|'STOPPED'|'SUCCEEDED'|'FAILED'|'TIMEOUT'
            if (job_status in ['STARTING','RUNNING']):
                time.sleep(self.polling_interval)
            elif (job_status in ['STOPPING','STOPPED','FAILED','TIMEOUT']):
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
            polling_interval,
            *args,
            **kwargs
    ):
        """
        Trigger AWS Glue Workflow function

        :param workflow_name: the name of the Glue workflow to start and monitor
        :param polling_interval: time interval, in seconds, to check the status of the workflow
        """
        super(StartGlueWorkflowRunOperator, self).__init__(*args, **kwargs)
        self.workflow_name = workflow_name
        self.polling_interval = polling_interval
        self.glue_client = boto3.client('glue')

    def execute(self, context):
        start_glue_workflow_response = self.glue_client.start_workflow_run(Name=self.workflow_name)
        logging.info(start_glue_workflow_response)

        glue_workflow_id = start_glue_workflow_response['RunId']
        logging.info("Glue Workflow ID: " + str(glue_workflow_id))

        while True:
            workflow_status = self.glue_client.get_workflow_run(Name=self.workflow_name,RunId=glue_workflow_id)['Run']['Status']
            logging.info("Workflow Status: " + str(workflow_status))

            # Possible values --> 'Status': 'RUNNING'|'COMPLETED'
            if (workflow_status in ['STARTING','RUNNING']):
                time.sleep(self.polling_interval)
            elif (workflow_status in ['STOPPING','STOPPED','FAILED','TIMEOUT']):
                logging.error("Something went wrong. Check AWS Logs. Exiting.") 
                raise AirflowException('AWS Glue Workflow Run Failed')
            else:
                break

class StartGlueCrawlerRunOperator(BaseOperator):

    ui_color = '#ff9900'

    @apply_defaults
    def __init__(
            self,
            crawler_name,
            polling_interval,
            *args,
            **kwargs
    ):
        """
        Trigger AWS Glue Crawler function

        :param crawler_name: the name of the Glue crawler to start and monitor
        :param polling_interval: time interval, in seconds, to check the status of the crawler
        """
        super(StartGlueCrawlerRunOperator, self).__init__(*args, **kwargs)
        self.crawler_name = crawler_name
        self.polling_interval = polling_interval
        self.glue_client = boto3.client('glue')

    def execute(self, context):

        # Retrieving last_crawl details to compare with later
        last_crawl_before_starting = self.glue_client.get_crawler(Name=self.crawler_name)['Crawler']['LastCrawl']

        start_glue_crawler_response = self.glue_client.start_crawler(Name=self.crawler_name)
        logging.info(start_glue_crawler_response)

        while True:
            crawler_status = self.glue_client.get_crawler(Name=self.crawler_name)['Crawler']['State']
            logging.info("Crawler Status: " + str(crawler_status))

            # Possible values --> 'State': 'READY'|'RUNNING'|'STOPPING'
            if (crawler_status in ['RUNNING']): 
                time.sleep(self.polling_interval)
            elif (crawler_status in ['STOPPING', 'READY']):
                last_crawl_at_stopping = self.glue_client.get_crawler(Name=self.crawler_name)['Crawler']['LastCrawl']
                if (last_crawl_before_starting == last_crawl_at_stopping):
                    time.sleep(self.polling_interval)
                else:
                    final_status = self.glue_client.get_crawler(Name=self.crawler_name)['Crawler']['LastCrawl']['Status']

                    # Possible values --> 'Status': 'SUCCEEDED'|'CANCELLED'|'FAILED'
                    if (final_status in ['SUCCEEDED']):
                        break
                    else:
                        logging.error("Something went wrong. Check AWS Logs. Exiting.") 
                        raise AirflowException('AWS Crawler Job Run Failed')

