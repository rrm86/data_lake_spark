'''
This file implements the
ETL process.
'''
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import hour, weekofyear, dayofweek
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''Create a spark session and add the aws package'''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Process song data.
    Load Song Data, transform 
    and create songs and artists
    parquet files.

    Parameters:
    spark : Spark Session
    input_data (str): Path to input data
    output_data (str): Path to output data
    '''
    # get filepath to song data file
    song_data = '{0}song_data/*/*/*/*.json'.format(input_data)
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']).parquet('{0}/songs.parquet'.format(output_data))
    

    # extract columns to create artists table
    artists_table =  df.select('artist_id', col('artist_name').alias('name'),\
                               col('artist_location').alias('location'),\
                               col('artist_latitude').alias('latitude'),\
                               col('artist_longitude').alias('longitude'))
    
    # write artists table to parquet files
    artists_table.write.parquet("{0}/artists.parquet".format(output_data))
    


def process_log_data(spark, input_data, output_data):
    '''
    Process log data.
    Load Log Data, transform 
    and create users, time
    and songplays parquet files.

    Parameters:
    spark : Spark Session
    PostgreSQL command in a database session
    input_data (str): Path to input data
    output_data (str): Path to output data
    '''
    # get filepath to log data file
    log_data = "{0}log_data/*/*/*.json".format(input_data)

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table 
    
    users_table = df.select(col('userId').alias('user_id'),\
                            col('firstName').alias('first_name'),\
                            col('lastName').alias('last_name'),\
                            'gender', 'level').distinct()
    
    # write users table to parquet files
    users_table.write.parquet("{0}/users.parquet".format(output_data))
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: (int(x)/1000.0))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    @udf(T.StringType())
    def get_datetime(timestamp):
        return datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    
    df = df.withColumn('datetime', get_datetime(df.timestamp))
    
    # extract columns to create time table
    time_table = df.select(col('datetime').alias('start_time'),\
                           hour('datetime').alias('hour'),\
                           dayofmonth('datetime').alias('day'),\
                           weekofyear('datetime').alias('week'),\
                           month('datetime').alias('month'),\
                           year('datetime').alias('year'),\
                           dayofweek('datetime').alias('weekday')).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year', 'month']).parquet('{0}/time.parquet'.format(output_data))

    # read in song data to use for songplays table
    song_df = spark.read.parquet('{0}/songs.parquet/').format(output_data)

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df, song_df.title == df.song) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table = df.select(col('datetime').alias('start_time'),\
                                col('userId').alias('user_id'),\
                                'level','song_id', 'artist_id',\
                                col('sessionId').alias('session_id'),\
                                'location',\
                                col('userAgent').alias('user_agent'))
    
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    songplays_table.write.parquet('{0}/songplays.parquet'.format(output_data))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://rrm86p4/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
