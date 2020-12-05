from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 append_data="",
                 *args, 
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append_data = append_data

    def execute(self, context):
        '''
            Copy data from staging table to fact table in Redshift
            
            Parameters:
                1) redshift_conn_id: redshift cluster connection
                2) table: fact table name
                3) sql_query: sql query to insert data
        '''
        
        self.log.info('LoadFactOperator - start')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)  

        self.log.info(f'LoadFactOperator - executing query for table {self.table}')
        if self.append_data == True:
            self.log.info('LoadFactOperator - append-only')
            redshift.run(self.sql_query)
        else:
            self.log.info('LoadFactOperator - delete-load')
            redshift.run('DELETE FROM {}'.format(self.table))
        
        self.log.info('LoadFactOperator - complete')
