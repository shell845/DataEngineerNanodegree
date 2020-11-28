import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType
from datetime import datetime
from pyspark.sql.functions import monotonicallyIncreasingId

"""
    Read configuration files
"""
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create spark session
        Or get existing spark session
    """
    spark = SparkSession
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        This function extract song data files from S3,
        transform to songs and artists tables, 
        output as parquet files and load back to S3
        
        Parameters:
            spark: spark session
            input_data: S3 path of input log files
            output_data: S3 path of output parquet files
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    # for testing
    song_data = input_data + 'song_data/A/D/O/*.json'
    
    
    songSchema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", IntegerType())
    ])
    
    df = spark.read.json(song_path_local, schema=songSchema)
   
    # extract columns to create songs table
    song_fields = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = df.select(song_fields)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
        This function extract log data files from S3,
        transform to songplays, users and time tables, 
        output as parquet files and load back to S3
        
        Parameters:
            spark: spark session
            input_data: S3 path of input log files
            output_data: S3 path of output parquet files
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'
    # for testing
    log_data = input_data + 'log_data/2018/11/2018-11-27-events.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    dfNextSong = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    dfNextSong = dfNextSong.withColumn("userId", dfNextSong["userId"].cast(IntegerType()))
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = dfNextSong.selectExpr(users_fields)
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    dfNextSong = dfNextSong.withColumn("timestamp", get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0), DateType())
    dfNextSong = dfNextSong.withColumn("start_time", get_datetime('ts'))
    
    # extract columns to create time table
    time_table = dfNextSong.select("start_time").dropDuplicates() \
        .withColumn("hour", hour(col("start_time"))).withColumn("day", dayofmonth(col("start_time"))) \
        .withColumn("week", weekofyear(col("start_time"))).withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time"))).withColumn("weekday", date_format(col("start_time"), 'E'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs/*/*/*')
    artists_df = spark.read.parquet(output_data + 'artists/*')
    
    # join
    songs_logs = dfNextSong.join(song_df, (dfNextSong.song == song_df.title))
    
    artists_songs_logs = songs_logs.join(artists_df, (songs_logs.artist == artists_df.name)).drop(artists_df.location)
    
    songplays = artists_songs_logs.join(time_table, (artists_songs_logs.start_time == time_table.start_time), 'left').drop(artists_songs_logs.start_time)
    
    songplays = songplays.withColumn("songplay_id", F.monotonically_increasing_id())

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays.select(
        col("songplay_id"),
        col('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId'),
        col('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month')
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays_local/')


def main():
    """
        Main function to:
            1. Extract song data and log data from S3
            2. Transform data from JSON to Fact and Dimension tables
            3. Load back to S3 as Parquet files
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
