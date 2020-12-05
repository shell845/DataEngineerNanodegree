from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 append_data="",
                 *args, 
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append_data = append_data

    def execute(self, context):
        '''
        Copy data from staging table to dimension tables in Redshift
            
            Parameters:
                1) redshift_conn_id: redshift cluster connection
                2) table: dimension table name
                3) sql_query: sql query to insert data
        '''
        
        self.log.info('LoadDimensionOperator - start')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)  

        self.log.info(f'LoadDimensionOperator - executing query for table {self.table}')
        if self.append_data == True:
            self.log.info('LoadDimensionOperator - append-only')
            redshift.run(self.sql_query)
        else:
            self.log.info('LoadDimensionOperator - delete-load')
            redshift.run('DELETE FROM {}'.format(self.table))
        
        
        self.log.info('LoadDimensionOperator - complete')
