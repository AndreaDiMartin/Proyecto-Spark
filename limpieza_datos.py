from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import timestamp_millis, col, year, month, weekday,day

schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("disc_number", IntegerType(), True),
        StructField("duration", LongType(), True),
        StructField("explicit", StringType(), True),
        StructField("audio_feature_id", StringType(), True),
        StructField("preview_url", StringType(), True),
        StructField("track_number", IntegerType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("is_playable", StringType(), True),
        StructField("acousticness", DoubleType(), True),
        StructField("danceability", DoubleType(), True),
        StructField("energy", DoubleType(), True),
        StructField("instrumentalness", DoubleType(), True),
        StructField("key", IntegerType(), True),
        StructField("liveness", DoubleType(), True),
        StructField("loudness", DoubleType(), True),
        StructField("mode", IntegerType(), True),
        StructField("speechiness", DoubleType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("time_signature", IntegerType(), True),
        StructField("valence", DoubleType(), True),
        StructField("album_name", StringType(), True),
        StructField("album_group", StringType(), True),
        StructField("album_type", StringType(), True),
        StructField("release_date", LongType(), True),
        StructField("album_popularity", IntegerType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_popularity", IntegerType(), True),
        StructField("followers", IntegerType(), True),
        StructField("genre_id", StringType(), True)
    ]
)

df = spark.read.schema(schema).option("header","true").csv("file:///home/hadoop/tracks-sample.csv")

#Drop rows with null values
df_no_null = df.filter(df["valence"].isNotNull())

#Drop unnecessary columns
df_necessary_columns = df_no_null.drop("id","disc_number" , "audio_feature_id", "preview_url", "track_number", "is_playable", "mode", "album_group")

#Change null genres to "unknown"
fill_cols_vals = {"genre_id": "unknown"}
df_no_empty_genres = df_necessary_columns.fillna(fill_cols_vals)

#Transform release date to normal date
df_transformed_date = df_no_empty_genres.withColumn("release_date", timestamp_millis("release_date"))
df_transformed_date = df_transformed_date.withColumn("year_of_release", year(col("release_date")))
df_transformed_date = df_transformed_date.withColumn("month_of_release", month(col("release_date")))
df_transformed_date = df_transformed_date.withColumn("weekday_of_release", weekday(col("release_date")))
df_transformed_date = df_transformed_date.withColumn("day_of_release", day(col("release_date")))

#Drop the release date column
df_final = df_transformed_date.drop("release_date")

#Save
df_final.write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("file:///home/hadoop/cleaned_tracks.csv")
