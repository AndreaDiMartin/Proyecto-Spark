#Most common genre in every year

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, sentences, element_at,expr, when, row_number
from pyspark.sql.window import Window


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
df_necessary_columns = df.select("genre_id", "year_of_release")
df_necessary_columns = df_necessary_columns.filter(df_necessary_columns["year_of_release"] >= 1900)
df_necessary_columns = df_necessary_columns.filter(df_necessary_columns["genre_id"] != "unknown")



#Group main genre

df_main_genre = df_necessary_columns.withColumn("main_genre_arr",sentences(col("genre_id")))
df_main_genre = df_main_genre.withColumn("main_genre",element_at(col("main_genre_arr"),-1))
df_main_genre = df_main_genre.withColumn("main_genre",element_at(col("main_genre"),-1))


df_main_genre = df_main_genre.withColumn(
    "final_genre",
    when(col("main_genre").isin(
            "hop", "country", "rock", "jazz", "pop", "reggae", "metal", "blues", "rap", "classical", "house", "folk", "dance",
            "r&b", "indie", "punk", "electronic", "hardcore", "trap"), col("main_genre")).otherwise(col("genre_id"))
)

#Drop columns
df_necessary_columns = df_main_genre.select("year_of_release", "final_genre")

#Group by year
df_group = df_main_genre.groupBy("year_of_release", "final_genre").count()

# Define a window partitioned by year_of_release and ordered by count in descending order
window_spec = Window.partitionBy("year_of_release").orderBy(col("count").desc())

# Add a row number column to identify the top genre per year
df_with_row_number = df_group.withColumn("row_number", row_number().over(window_spec))

# Filter to keep only the top genre (row_number = 1) for each year
df_max_per_year = df_with_row_number.filter(col("row_number") == 1).select("year_of_release", "final_genre", "count")

# Save
df_max_per_year.write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("file:///home/hadoop/common_genres1")

################# VERSION SQL ###################
#Create table
spark.sql("CREATE TABLE spotify_csv ( track_name STRING, duration BIGINT, explicit STRING, popularity INT,acousticness DOUBLE, danceability DOUBLE, energy DOUBLE, instrumentalness DOUBLE, key INT,liveness DOUBLE,loudness DOUBLE,speechiness DOUBLE,tempo DOUBLE,time_signature INT,valence DOUBLE,album_name STRING,album_type STRING,album_popularity INT,artist_name STRING,artist_popularity INT,followers INT,genre_id STRING,year_of_release INT,month_of_release INT,weekday_of_release INT,day_of_release INT) USING csv OPTIONS (header true, path 'file:///home/hadoop/cleaned_tracks.csv')")

#Select only the necessary columns
spark.sql("CREATE TABLE spotify_necessary_columns AS SELECT genre_id, year_of_release FROM spotify_csv WHERE year_of_release >= 1900 AND genre_id != 'unknown' ")

#Group main genre
spark.sql("CREATE TABLE spotify_genres_split AS SELECT sentences(genre_id) AS main_genre_arr, year_of_release,genre_id FROM spotify_necessary_columns")
spark.sql("CREATE TABLE spotify_genres_divide AS SELECT element_at(element_at(main_genre_arr,-1),-1) AS main_genre, year_of_release,genre_id FROM spotify_genres_split")
spark.sql("CREATE TABLE spotify_main_genre AS SELECT CASE WHEN main_genre IN ('hop', 'country', 'rock', 'jazz', 'pop', 'reggae', 'metal', 'blues', 'rap', 'classical', 'house', 'folk', 'dance', 'r&b', 'indie', 'punk', 'electronic', 'hardcore', 'trap') THEN main_genre ELSE genre_id END AS final_genre,year_of_release FROM spotify_genres_divide")

#Group by year
spark.sql("CREATE TABLE spotify_common_genres AS WITH RankedGenres AS (SELECT year_of_release,final_genre,COUNT(*) AS genre_count,ROW_NUMBER() OVER (PARTITION BY year_of_release ORDER BY COUNT(*) DESC) AS rank FROM spotify_main_genre GROUP BY year_of_release, final_genre) SELECT year_of_release,  final_genre, genre_count FROM RankedGenres WHERE rank = 1")
spark.sql("CREATE TABLE spotify_common_genres AS SELECT year_of_release, final_genre, genre_count FROM RankedGenres WHERE rank = 1")