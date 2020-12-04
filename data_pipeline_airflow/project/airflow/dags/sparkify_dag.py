from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries 

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'shell845',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 2),
    'retries': 1, # set to 3 for submission
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *' # corn format for daily
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_tables.sql',
  postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    s3_region='us-west-2',
    file_format='JSON',
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    s3_region='us-west-2',
    file_format='JSON',
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator