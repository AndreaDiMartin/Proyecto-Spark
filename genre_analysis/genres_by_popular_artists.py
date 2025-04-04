from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, sentences, element_at,expr, when, desc, asc

schema_clean = StructType(
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

df = spark.read.schema(schema_clean).option("header","true").csv("file:///home/hadoop/cleaned_tracks.csv")

#Select only the necessary columns
df_necessary_columns = df.select("genre_id", "artist_popularity")

#Use only popular columns
df_popular_artists = df_necessary_columns.filter(df_necessary_columns["artist_popularity"] > 70)

#Group by main genre
df_main_genre = df_popular_artists.withColumn("main_genre_arr",sentences(col("genre_id")))
df_main_genre = df_main_genre.withColumn("main_genre",element_at(col("main_genre_arr"),-1))
df_main_genre = df_main_genre.withColumn("main_genre",element_at(col("main_genre"),-1))

df_main_genre = df_main_genre.withColumn(
    "final_genre",
    when(col("main_genre").isin(
            "hop", "country", "rock", "jazz", "pop", "reggae", "metal", "blues", "rap", "classical", "house", "folk", "dance",
            "r&b", "indie", "punk", "electronic", "hardcore", "trap"), col("main_genre")).otherwise(col("genre_id"))
)


#Group by genre
df_group = df_main_genre.groupBy("final_genre").agg(expr("count(artist_popularity)"))

#Sorting
df_final = df_group.orderBy(col("count(artist_popularity)").desc())

#Save
df_final.write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("file:///home/hadoop/popular_genres")

####################### VERSION SQL####################

#Create table
spark.sql("CREATE TABLE spotify_csv ( track_name STRING, duration BIGINT, explicit STRING, popularity INT,acousticness DOUBLE, danceability DOUBLE, energy DOUBLE, instrumentalness DOUBLE, key INT,liveness DOUBLE,loudness DOUBLE,speechiness DOUBLE,tempo DOUBLE,time_signature INT,valence DOUBLE,album_name STRING,album_type STRING,album_popularity INT,artist_name STRING,artist_popularity INT,followers INT,genre_id STRING,year_of_release INT,month_of_release INT,weekday_of_release INT,day_of_release INT) USING csv OPTIONS (header true, path 'file:///home/hadoop/cleaned_tracks_sample.csv')")

#Select only the necessary columns
spark.sql("CREATE TABLE spotify_necessary_columns AS SELECT genre_id, artist_popularity FROM spotify_csv WHERE artist_popularity >= 70")

#Get main genre
spark.sql("CREATE TABLE spotify_genres_split AS SELECT sentences(genre_id) AS main_genre_arr, artist_popularity,genre_id FROM spotify_necessary_columns")
spark.sql("CREATE TABLE spotify_genres_divide AS SELECT element_at(element_at(main_genre_arr,-1),-1) AS main_genre, artist_popularity,genre_id FROM spotify_genres_split")
spark.sql("CREATE TABLE spotify_main_genre AS SELECT CASE WHEN main_genre IN ('hop', 'country', 'rock', 'jazz', 'pop', 'reggae', 'metal', 'blues', 'rap', 'classical', 'house', 'folk', 'dance', 'r&b', 'indie', 'punk', 'electronic', 'hardcore', 'trap') THEN main_genre ELSE genre_id END AS final_genre,artist_popularity FROM spotify_genres_divide")

#Group by genre
spark.sql("CREATE TABLE spotify_count_genres AS SELECT final_genre, COUNT(artist_popularity) AS count FROM spotify_main_genre GROUP BY final_genre SORT BY count desc")