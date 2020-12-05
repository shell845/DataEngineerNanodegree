from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables="",
                 sql_query="",
                 *args,
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.sql_query = sql_query

    def execute(self, context):
        '''
        Data quality check for fact and dimension tables
            
            Parameters:
                1) redshift_conn_id: redshift cluster connection
                2) tables: list of fact and dimension tables
        '''
        
        self.log.info(f'DataQualityOperator - start to check data quality of {self.tables}')
        
        redshift = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            sql_query_formatted = self.sql_query.format(table)
            self.log.info(f"Running data quality check query: {sql_query_formatted}")
            records = redshift.get_records(sql_query_formatted)
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"DataQualityOperator - {table} is empty")
                raise ValueError(f"DataQualityOperator - {table} is empty")
            else:
                self.log.info(f"DataQualityOperator - {table} has {records[0][0]} records")
        
        self.log.info('DataQualityOperator - complete')