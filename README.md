# airflow-aws-plugin

Airflow plugin with AWS operators

# Table of contents

- [Installation](#installation)
- [Operators](#operators)
    - [AWS Lambda](#aws-lambda)
        - [ExecuteLambdaOperator](#executelambdaoperator)
    - [AWS Redshift](#aws-redshift)    
        - [ExecuteRedshiftQueryOperator](#executeredshiftqueryoperator)
        - [ExecuteCopyToRedshiftOperator](#executecopytoredshiftoperator)   
    - [AWS Glue](#aws-glue)    
        - [StartGlueJobRunOperator](#startgluejobrunoperator)
        - [StartGlueWorkflowRunOperator](#startglueworkflowrunoperator)
        - [StartGlueCrawlerOperator](#startgluecrawleroperator)
        - [StartGlueTriggerOperator](#startgluetriggeroperator)
    - [AWS Database Migration Service (DMS)](#aws-database-migration-service-dms)
        - [StartDMSReplicationTaskOperator](#startdmsreplicationtaskoperator)

# Installation

Copy [aws_operators](/aws_operators) directory to *plugins* directory in airflow (default AIRFLOW_HOME/plugins/).

# Operators

List of operators by AWS service:

## AWS Lambda

### ExecuteLambdaOperator

Operator responsible for triggering AWS Lambda function.

**Example**

```python
ExecuteLambdaOperator(
    task_id='task_with_execute_lambda_operator',
    airflow_context_to_lambda_payload=lambda c: {"date": c["execution_date"].strftime('%Y-%m-%d')   },
    additional_payload={"param1": "value1", "param2": 21},
    lambda_function_name="LambdaFunctionName"
)
```

Above task executes AWS Lambda function `LambdaFunctionName` with payload:

```json
{
  "date": "2018-08-01",
  "param1": "value1",
  "param2": 21
}
```
where `date` is equal to `execution_date` of airflow dag. This is extracted by `airflow_context_to_lambda_payload` function from airflow context dictionary.

## AWS Redshift

### ExecuteRedshiftQueryOperator

Execute Redshift query.

**Example**

DROP Redshift table:

```python
ExecuteRedshiftQueryOperator(
    task_id='drop_table',
    redshift_conn_id='redshift_dev',
    query='DROP TABLE IF EXISTS TEST_TABLE'
)
```

#### Query depending on execution date

Query can be constructed based on Airflow context, especially execution date.

Example:
```python
ExecuteRedshiftQueryOperator(
    task_id='delete_from_table',
    redshift_conn_id='redshift_dev',
    query=lambda c: "DELETE FROM TABLE TEST_TABLE WHERE MONTH = '{y}-{m}'".format(y=c["execution_date"].year, m=c["execution_date"].strftime("%m"))
)
```

### ExecuteCopyToRedshiftOperator

Execute Redshift COPY command.

*Example 1 - append data:*

```python
ExecuteCopyToRedshiftOperator(
    task_id='redshift_copy_append',
    redshift_conn_id='redshift_dev',
    s3_bucket='bucket',
    s3_key='key',
    redshift_schema='public',
    table='table',
    iam_role='iam_role',
    mode='append'
)
```

*Example 2 - overwrite table:*

```python
ExecuteCopyToRedshiftOperator(
    task_id='redshift_copy_overwrite',
    redshift_conn_id='redshift_dev',
    s3_bucket='bucket',
    s3_key='key',
    redshift_schema='public',
    table='table',
    iam_role='iam_role',
    mode='overwrite',
    copy_params=['CSV']
)
```

### ExecuteUnloadFromRedshiftOperator

Execute Redshift UNLOAD command.

```python
ExecuteUnloadFromRedshiftOperator(
    task_id='redshift_unload',
    redshift_conn_id='redshift_dev',
    select_statement='SELECT * FROM TABLE',
    s3_bucket='bucket',
    s3_key='key',
    iam_role='iam_role',
    unload_params=["DELIMITER AS ';'", "GZIP"]
)
```

#### S3 key dependent on airflow context

Source S3 key can be constructed using custom Python function based on airflow context.

Example:

```python
ExecuteCopyToRedshiftOperator(
    s3_key=lambda context: "year={y}/month={m}/day={d}/".format(y=context["execution_date"].year, m=context["execution_date"].strftime("%m"), d=context["execution_date"].strftime("%d"))
)
```

## AWS Glue

### StartGlueJobRunOperator

Operator responsible for starting and monitoring Glue jobs.

**Parameters**

```
job_name (string) [REQUIRED] -- the name of the Glue job to start and monitor
polling_interval (integer) (default: 10) -- time interval, in seconds, to check the status of the job
job_run_id (string) -- The ID of a previous JobRun to retry.
arguments (dict) -- The job arguments specifically for this run. For this job run, they replace the default arguments set in the job definition itself.
timeout (integer) -- The JobRun timeout in minutes.
max_capacity (float) -- The number of AWS Glue data processing units (DPUs) that can be allocated when this job runs.
security_configuration (string) -- The name of the SecurityConfiguration structure to be used with this job run.
notification_property (dict) -- Specifies configuration properties of a job run notification.
worker_type (string) -- The type of predefined worker that is allocated when a job runs. Accepts a value of Standard, G.1X, or G.2X.
number_of_workers (integer) -- The number of workers of a defined workerType that are allocated when a job runs.

Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.start_job_run
```

**Example**

```python
glue_job_operator = StartGlueJobRunOperator(
    task_id='glue_job_operator',
    job_name='airflow',
    polling_interval=10,
    dag=dag
)
```

### StartGlueWorkflowRunOperator

Operator responsible for starting and monitoring Glue workflows.

**Parameters**

```
workflow_name (string) [REQUIRED]: the name of the Glue workflow to start and monitor
polling_interval (integer) (default: 10) -- time interval, in seconds, to check the status of the job
```

**Example**

```python
glue_workflow_operator = StartGlueWorkflowRunOperator(
    task_id='glue_workflow_operator',
    workflow_name='airflow',
    polling_interval=10,
    dag=dag
)
```

### StartGlueCrawlerOperator

Operator responsible for starting and monitoring Glue crawlers.

**Parameters**

```
crawler_name (string) [REQUIRED]: the name of the Glue crawler to start and monitor
polling_interval (integer) (default: 10) -- time interval, in seconds, to check the status of the job
```

**Example**

```python
glue_crawler_operator = StartGlueCrawlerOperator(
    task_id='glue_crawler_operator',
    crawler_name='airflow',
    polling_interval=10,
    dag=dag
)
```

### StartGlueTriggerOperator

Operator responsible for starting AWS Glue triggers.

**Parameters**

```
trigger_name (string) [REQUIRED]: the name of the Glue trigger to start
```

**Example**

```python
glue_trigger_operator = StartGlueTriggerOperator(
    task_id='glue_trigger_operator',
    trigger_name='airflow',
    dag=dag
)
```

## AWS Database Migration Service (DMS)

### StartDMSReplicationTaskOperator

Operator responsible for starting a DMS replication task.

**Parameters**

```
replication_task_arn (string) [REQUIRED] -- The Amazon Resource Name (ARN) of the replication task to be started
start_replication_task_type (string) [REQUIRED] -- The type of replication task. Possible Values include start-replication, resume-processing, reload-target
polling_interval (integer) (default: 10) -- time interval, in seconds, to check the status of the job
cdc_start_time (datetime) -- Indicates the start time for a change data capture (CDC) operation. Use either CdcStartTime or CdcStartPosition to specify when you want a CDC operation to start. Specifying both values results in an error.
cdc_start_position (string) -- Indicates when you want a change data capture (CDC) operation to start. Use either CdcStartPosition or CdcStartTime to specify when you want a CDC operation to start. Specifying both values results in an error.
cdc_stop_position (string) -- Indicates when you want a change data capture (CDC) operation to stop. The value can be either server time or commit time.

Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dms.html#DatabaseMigrationService.Client.start_replication_task
```

**Example**

```python
dms_replication_task_operator = StartDMSReplicationTaskOperator(
    task_id='dms_replication_task_operator',
    replication_task_arn='airflow',
    start_replication_task_type='start-replication',
    polling_interval=10,
    dag=dag
)
```