#### Data Engineering Nanodegree Project 4
# Airflow

## Project summary
In this project, we use Airflow to manage ELT pipeline:
1. Load data files from S3 to Redshift staging tables.
2. Transform data in staging tables and load into fact and dimension tables in Redshift.
3. Conduct data quality check.

## Project structure

## DAG diagram
![DAG](https://github.com/shell845/DataEngineerNanodegree/blob/master/data_pipeline_airflow/reference/sparkify_dag.png)

## Data set
- Log data `s3://udacity-dend/log_data`
- Song data `s3://udacity-dend/song_data`

