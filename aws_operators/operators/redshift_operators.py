from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils import apply_defaults
import logging


class ExecuteRedshiftQueryOperator(BaseOperator):

    @apply_defaults
    def __init__(self, redshift_conn_id, query, *args, **kwargs):
        """
        Execute Redshift query

        :param redshift_conn_id: the destination redshift connection id
        :param query: SQL query to execute - can be string or function converting airflow context to query
        """
        super(ExecuteRedshiftQueryOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        pg_hook = PostgresHook(self.redshift_conn_id)
        query = self.query if type(self.query) == str else self.query(context)
        logging.info("Execute Redshift query {}".format(query))
        pg_hook.run(query)


class DropRedshiftTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self, redshift_conn_id, full_table_name, *args, **kwargs):
        """
        DROP Redshift table

        :param redshift_conn_id: the destination redshift connection id
        :param full_table_name: full Redshift table name to drop
        """
        super(DropRedshiftTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.full_table_name = full_table_name

    def execute(self, context):
        pg_hook = PostgresHook(self.redshift_conn_id)
        pg_hook.run("DROP TABLE IF EXISTS " + self.full_table_name)


class TruncateRedshiftTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self, redshift_conn_id, full_table_name, *args, **kwargs):
        """
        TRUNCATE Redshift table

        :param redshift_conn_id: the destination redshift connection id
        :param full_table_name: full Redshift table name to truncate
        """
        super(TruncateRedshiftTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.full_table_name = full_table_name

    def execute(self, context):
        pg_hook = PostgresHook(self.redshift_conn_id)
        pg_hook.run("TRUNCATE TABLE " + self.full_table_name)


class VacuumRedshiftTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self, redshift_conn_id, full_table_name, *args, **kwargs):
        """
        VACUUM Redshift table

        :param redshift_conn_id: the destination redshift connection id
        :param full_table_name: full Redshift table name to vacuum
        """
        super(VacuumRedshiftTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.full_table_name = full_table_name

    def execute(self, context):
        pg_hook = PostgresHook(self.redshift_conn_id)
        query = "VACUUM FULL {}".format(self.full_table_name)
        # Using connection, because VACUUM can't be executed in transaction and pg_hook is executing within transaction
        conn = pg_hook.get_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(query)


class ExecuteCopyToRedshiftOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id,
            s3_bucket,
            s3_key,
            redshift_schema,
            redshift_table,
            iam_role,
            mode,
            where_condition_fn=None,
            copy_params=[],
            *args,
            **kwargs
    ):
        """
        Execute Redshift COPY command

        Modes:
            * append - just insert new rows to table
            * overwrite - truncate table and insert new rows
            * append_overwrite - remove selected rows using condition where_condition and then insert new rows

        :param redshift_conn_id: the destination redshift connection id
        :param s3_bucket: name of source S3 bucket
        :param s3_key: path to source data in S3 bucket - can be string or function converting airflow context to path (e.g. to have different path depending on execution date)
        :param redshift_schema: name of destination Redshift schema
        :param redshift_table: name of destination Redshift table
        :param iam_role: name of IAM role for Redshift COPY command
        :param mode: append, overwrite, append_overwrite
        :param where_condition_fn: obligatory parameter for append_overwrite mode, function returning condition for WHERE statement in delete
        :param copy_params: additional COPY command parameters
        """

        super(ExecuteCopyToRedshiftOperator, self).__init__(*args, **kwargs)
        self.pg_hook = PostgresHook(redshift_conn_id)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_schema = redshift_schema
        self.redshift_table = redshift_table
        self.full_table_name = self.redshift_schema + "." + self.redshift_table
        self.iam_role = iam_role
        self.mode = mode.upper()
        self.where_condition_fn = where_condition_fn
        self.copy_params = copy_params

    def execute(self, context):
        if self.mode == "OVERWRITE":
            self.__truncate_table()
            self.__execute_copy(context)
        elif self.mode == "APPEND":
            self.__execute_copy(context)
        elif self.mode == "APPEND_OVERWRITE":
            self.__delete_from_table(context)
            self.__execute_copy(context)
            self.__vacuum_table()

    def __execute_query(self, query):
        print("Executing query: " + query)
        self.pg_hook.run(query)

    def __vacuum_table(self):
        query = "VACUUM FULL {}".format(self.full_table_name)
        # Using connection, because VACUUM can't be executed in transaction and pg_hook is executing within transaction
        conn = self.pg_hook.get_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(query)

    def __truncate_table(self):
        query = "TRUNCATE TABLE {}".format(self.full_table_name)
        self.__execute_query(query)

    def __delete_from_table(self, context):
        condition = self.where_condition_fn(context)
        query = "DELETE FROM {} WHERE {}".format(self.full_table_name, condition)
        self.__execute_query(query)

    def __execute_copy(self, context):
        copy_query = self.__construct_copy_query(context)
        self.__execute_query(copy_query)

    def __construct_copy_query(self, context):
        additional_params = '\n'.join(self.copy_params)
        s3_key = self.s3_key if type(self.s3_key) == str else self.s3_key(context)
        return """
        COPY {table}
        FROM 's3://{bucket}/{key}'
        CREDENTIALS 'aws_iam_role={iam_role}'
        {additional_params}
        """.format(
            table=self.full_table_name,
            bucket=self.s3_bucket,
            key=s3_key,
            iam_role=self.iam_role,
            additional_params=additional_params
        )


class ExecuteUnloadFromRedshiftOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id,
            select_statement,
            s3_bucket,
            s3_key,
            iam_role,
            unload_params=[],
            *args,
            **kwargs
    ):
        """
        Execute Redshift UNLOAD command

        :param redshift_conn_id: the destination redshift connection id
        :param select_statement: select statement to unload data
        :param s3_bucket: name of destination S3 bucket
        :param s3_key: path to statement data in S3 bucket - can be string or function converting airflow context to path (e.g. to have different path depending on execution date)
        :param iam_role: name of IAM role for Redshift UNLOAD command
        :param unload_params: additional UNLOAD command parameters
        """

        super(ExecuteUnloadFromRedshiftOperator, self).__init__(*args, **kwargs)
        self.pg_hook = PostgresHook(redshift_conn_id)
        self.select_statement = select_statement
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.iam_role = iam_role
        self.unload_params = unload_params

    def execute(self, context):
        additional_params = '\n'.join(self.unload_params)
        s3_key = self.s3_key if type(self.s3_key) == str else self.s3_key(context)
        query = """
        UNLOAD ('{select_statement}')
        TO 's3://{bucket}/{key}'
        iam_role '{iam_role}'
        {additional_params}
        """.format(
            select_statement=self.select_statement,
            bucket=self.s3_bucket,
            key=s3_key,
            iam_role=self.iam_role,
            additional_params=additional_params
        )
        self.pg_hook.run(query)


class ExecuteAnalyzeRedshiftTableOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id,
            full_table_name,
            *args,
            **kwargs
    ):
        """
        Execute Redshift analyze table command

        :param redshift_conn_id: the destination redshift connection id
        :param full_table_name: full Redshift table name to analyze
        """
        super(ExecuteAnalyzeRedshiftTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.full_table_name = full_table_name

    def execute(self, context):
        pg_hook = PostgresHook(self.redshift_conn_id)
        pg_hook.run("ANALYZE " + self.full_table_name)
