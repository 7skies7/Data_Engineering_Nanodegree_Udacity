import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, from_unixtime, when
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql import types as T

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['ACCESS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['ACCESS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/A/*.json'

    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id','artist_name', 'year', 'duration']
    songs_table = songs_table.drop_duplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs/", mode="overwrite")

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
   
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    # read log data file
    df = spark.read.json(log_data)  
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    
    # extract columns for users table 
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level','ts'].dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/", mode="overwrite")
    
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)), T.TimestampType())
    df = df.withColumn("modified_ts", get_timestamp('ts'))
    
  
    
    # extract columns to create time table
    time_table = (
        df.withColumn("hour", hour(col("modified_ts")))
          .withColumn("day", dayofmonth(col("modified_ts")))
          .withColumn("week", weekofyear(col("modified_ts")))
          .withColumn("month", month(col("modified_ts")))
          .withColumn("year", year(col("modified_ts")))
          .withColumn("weekday", dayofweek(col("modified_ts")))
          .select(
            col("modified_ts").alias("start_time"),
            col("hour"),
            col("day"),
            col("week"),
            col("month"),
            col("year"),
            col("weekday")
          )
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time/", mode="overwrite")    
    
    # read in song data to use for songplays table
    songs_df = spark.read.parquet(output_data + 'songs')

    artists_df = spark.read.parquet(output_data + 'artists')

    songplays_cols = ["start_time", 
                      "userId as user_id",
                      "level",
                      "song_id",
                      "artist_id",
                      "sessionId as session_id",
                      "location",
                      "userAgent as user_agent",
                      "year",
                      "month"]
    songplays_table = (
        df
        .join(songs_df, df.song == songs_df.title)
        .drop("artist_id")
        .drop("year")
        .join(artists_df, df.artist == artists_df.artist_name)
        .join(time_table, df.modified_ts == time_table.start_time)
        
        .filter("song_id is not null and artist_id is not null")
        .selectExpr(*songplays_cols)
    )
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/', mode="overwrite")

    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-data-lake/"
    #output_data = ""
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)



if __name__ == "__main__":
    main()
