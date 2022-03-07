import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """"
    Create a Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """"
    Process song data files

    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs_table.parquet")

    # extract columns to create artists table
    artists_table = df.select("artist_id",
                              col("artist_name").alias("name"),
                              col("artist_location").alias("location"),
                              col("artist_latitude").alias("latitude"),
                              col("artist_longitude").alias("longitude")).dropDuplicates()
    
    # write artists table to parquet files
    artists_table = artists_table.write.parquet(output_data + "artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Process log data files
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"),
                            col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"), "gender", "level")
    
    # write users table to parquet files
    users_table = users_table.write.parquet(output_data + "users_table.parquet")

    # create timestamp column from original timestamp column
    @udf(returnType=TimestampType())
    def get_timestamp(ts):
        return datetime.fromtimestamp(ts/1000.0)

    # create start time column from original timestamp column
    df = df.withColumn("start_time", get_timestamp(df.ts))

    # extract columns to create time table
    df = df.withColumn("hour", hour(df.start_time))
    df = df.withColumn("day", dayofmonth(df.start_time))
    df = df.withColumn("week", weekofyear(df.start_time))
    df = df.withColumn("month", month(df.start_time))
    df = df.withColumn("year", year(df.start_time))
    df = df.withColumn("weekday", date_format(df.start_time, "E"))

    # extract columns to create time table
    time_table = df.select("start_time", "hour", "day", "week", "month", "year", "weekday")\
        .dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy("year", "month").parquet(output_data + "time_table.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table.parquet")
    artist_df = spark.read.parquet(output_data + "artists_table.parquet")

    # join song and artist tables
    song_artist_df = song_df.join(artist_df, "artist_id", 'left_outer')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_artist_df,
                              (df.song == song_artist_df.title) & (df.artist == song_artist_df.name)
                              & (df.length == song_artist_df.duration), 'left_outer')\
        .select(df.start_time, col(df.userId).alias("user_id"), df.level, song_artist_df.song_id,
                song_artist_df.artist_id, col(df.sessionId).alias("session_id"), df.location,
                col(df.userAgent).alias("user_agent"))\
        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays_table.parquet")


def main():
    """
    Main function
    """
    # create spark session
    spark = create_spark_session()

    # define input and output directories
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://eescobar-datasets/tables/"

    # process song data files
    process_song_data(spark, input_data, output_data)

    # process log data files
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
