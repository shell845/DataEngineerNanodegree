import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    copy data from S3 jason files to Redshift staging table
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    extract, transform and load data from Redshift staging table to fact tables and dimension tables
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    the main function to extract data from S3 json files and transform and load to Redshift
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Start ELT')
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print('End ELT')

    conn.close()


if __name__ == "__main__":
    main()