# Init

* SSH to EC2 instance with airflow

* Run below commands:


    export AIRFLOW_HOME=/home/airflow
    cd $AIRFLOW_HOME/airflow_virtualenv/bin
    source activate
    
* List dags:

    
    airflow list_dags
    
# Testing ExecuteLambdaOperator

* List tasks in dag with Lambda:

    
    airflow list_tasks dag_with_execute_lambda_operator
    
* Run tasks with lambda:    

    
    airflow test dag_with_execute_lambda_operator task_with_execute_lambda_operator 2018-08-10
 
# Testing ExecuteRedshiftQueryOperator
    
* List tasks in dag execute Redshift queries:


    airflow list_tasks dag_with_execute_redshift_operator
    

* Run create table task


    airflow test dag_with_execute_redshift_operator create_table 2018-08-10
     
* Run dag


    airflow backfill dag_with_execute_redshift_operator -s 2018-08-10 -e 2018-08-10

# Testing ExecuteCopyToRedshiftOperator

* List tasks in dag execute Redshift queries:


    airflow list_tasks dag_with_execute_copy_to_redshift_operator
    
* Run tasks


    airflow test dag_with_execute_copy_to_redshift_operator redshift_copy_overwrite 2018-08-10
    airflow test dag_with_execute_copy_to_redshift_operator redshift_copy_append 2018-08-10
    airflow test dag_with_execute_copy_to_redshift_operator redshift_copy_append_overwrite 2018-08-10
    
* Run dag


    airflow backfill dag_with_execute_copy_to_redshift_operator -s 2018-08-10 -e 2018-08-10
