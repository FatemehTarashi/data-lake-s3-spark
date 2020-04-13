import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id

# Read from dl.cfg file 
config = configparser.ConfigParser()
config.read('dl.cfg')

#Read access key and secret key to access AWS env
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    This function create spark session.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function process song data into songs, time and songplays table
    Arguments:
        spark: spark session
        input_data: S3 bucket path - json file
        output_data: S3 bucket path - parquet files
    '''
    # get filepath to song data file
    song_data = "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(input_data + song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id',
                            'title',
                            'artist_id',
                            'year',
                            'duration').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(partitionBy=['year','artist_id'],
                              path= output_data + 'songdata/song.parquet',
                              mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              'artist_name',
                              'artist_location',
                              'artist_latitude',
                              'artist_longitude').dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(path= output_data + 'artistdata/artist.parquet',
                                mode='overwrite')

    
def process_log_data(spark, input_data, output_data):
    '''
    This function process log data into user, and artist table
    Arguments:
        spark: spark session
        input_data: S3 bucket path - json file
        output_data: S3 bucket path - parquet files
    '''
    # get filepath to log data file
    log_data = "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(input_data + log_data)

    # filter by actions for song plays
    df =df.filter(df.page=='NextSong') 

    # extract columns for users table    
    user_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()

    # write users table to parquet files
    user_table.write.parquet(path= output_data + 'userdata/user.parquet',mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ms:datetime.fromtimestamp(ms/1000).strftime('%H:%M:%S'))
    df = df.withColumn('timestamp', get_timestamp(col('ts')))

    # create datetime column from original timestamp column
    get_datetime= udf(lambda ms:datetime.fromtimestamp(ms/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('start_time', get_timestamp(col('ts')))

    # extract columns to create time table
    time_table = df.select("start_time",\
                          hour('start_time').alias('hour'),\
                          dayofmonth('start_time').alias('day'),\
                          weekofyear('start_time').alias('week'),\
                          month('start_time').alias('month'),\
                          year('start_time').alias('year'),\
                          date_format('start_time', 'u').alias('weekday')).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(partitionBy=['year','month'],
                         path= output_data + 'timedata/time.parquet',
                         mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.artist==song_df.artist_name) & (df.song==song_df.title)).select('start_time',\
                              'userId','level','song_id','artist_id','sessionId','location','userAgent',\
                              month('start_time').alias('month'),\
                              year('start_time').alias('year')).withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(partitionBy=['year','month'],
                         path= output_data + 'songplaysdata/songplays.parquet',
                         mode='overwrite')

    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
