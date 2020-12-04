from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            {} 'auto ignorecase' 
            {}
        """
    
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 s3_region="",
                 file_format="",
                 *args, 
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.file_format = file_format
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        '''
            Copy data files to Redshift staging table from S3
            
            Parameters:
                1) redshift_conn_id: redshift cluster connection
                2) aws_credentials_id: AWS connection
                3) table: redshift staging table name
                4) s3_bucket: S3 bucket name holding source data
                5) s3_key: S3 key files of source data
                6) s3_region: S3 region
                7) file_format: S3 source file format, JSON or CSV
        '''
        
        self.log.info('StageToRedshiftOperator - start')        
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credential = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'StageToRedshiftOperator - clear Redshift stage table {self.table}')
        redshift.run('DELETE FROM {}'.format(self.table))
        
        self.log.info(f'StageToRedshiftOperator - compose S3 path')
        # S3 URI sample: s3://udacity-dend/log_data/2018/11/2018-11-01-events.json
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        if self.execution_date:
            year = self.execution_date.strftime("%Y")  # convert year to string
            month = self.execution_date.strftime("%m") # convert month to string
            # day = self.execution_date.strftime("%d")   # convert day to string
            s3_path = '/'.join([s3_path, str(year), str(month)])  # compose S3 path
        # s3_file = f'/*.{self.file_format}'   # compose S3 file name
        ''' testing - remove before submission'''
        s3_file = f'/2018-11-01-events.{self.file_format}'
        ''' end testing'''
        s3_path = s3_path + s3_file           # append s3 key

        # for csv files only
        convert_csv=""
        if self.file_format == 'CSV':
            convert_csv = " DELIMETER ',' IGNOREHEADER 1 "
        
        self.log.info(f'StageToRedshiftOperator - copy data from S3 {s3_path} to Redshift stage table {self.table}')  
        formated_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_credential.access_key,
            aws_credential.secret_key,
            self.s3_region,
            self.file_format,
            convert_csv
        )
        redshift.run(formated_sql)
        
        self.log.info('StageToRedshiftOperator - complete')





