from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, 
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        self.log.info('LoadFactOperator - start')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)  
        sql_stmt = f"{self.table}_table_insert"
        
        self.log.info(f'LoadFactOperator - executing {sql_stmt}')
        redshift.run(SqlQueries.sql_stmt)
        
        self.log.info('LoadFactOperator - complete')
