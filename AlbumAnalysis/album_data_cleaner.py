from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType


spark = SparkSession.builder.master("local[*]").getOrCreate()

input_schema = StructType(
    [
        StructField("track_name", StringType(), True),
        StructField("duration", LongType(), True),
        StructField("explicit", StringType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("acousticness", DoubleType(), True),
        StructField("danceability", DoubleType(), True),
        StructField("energy", DoubleType(), True),
        StructField("instrumentalness", DoubleType(), True),
        StructField("key", IntegerType(), True),
        StructField("liveness", DoubleType(), True),
        StructField("loudness", DoubleType(), True),
        StructField("speechiness", DoubleType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("time_signature", IntegerType(), True),
        StructField("valence", DoubleType(), True),
        StructField("album_name", StringType(), True),
        StructField("album_type", StringType(), True),
        StructField("album_popularity", IntegerType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_popularity", IntegerType(), True),
        StructField("followers", IntegerType(), True),
        StructField("genre_id", StringType(), True),
        StructField("year_of_release", IntegerType(), True),
        StructField("month_of_release", IntegerType(), True),
        StructField("weekday_of_release", IntegerType(), True),
        StructField("day_of_release", IntegerType(), True)

    ]
)

# read data
input_df = spark\
        .read\
        .schema(input_schema)\
        .option("header","true")\
        .csv("file:///home/hadoop/cleaned_tracks.csv")

# remove unnecessary columns
output_df = input_df.drop("track_name","duration","explicit","popularity","acousticness",\
                          "danceability","energy","instrumentalness","key","liveness","loudness",\
                          "speechiness","tempo","time_signature","valence", "artist_popularity",\
                          "followers","genre_id")

# write album data
output_df.\
        write\
        .format("csv")\
        .mode("overwrite")\
        .option("sep", ",")\
        .option("header", "true")\
        .save("file:///home/hadoop/Proyecto-Spark/AlbumAnalysis/album_data.csv")


spark.stop()