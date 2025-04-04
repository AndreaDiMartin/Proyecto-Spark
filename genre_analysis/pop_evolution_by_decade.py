from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, sentences, element_at,expr

#Genre throught the decades 

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
df_necessary_columns = df.select("year_of_release", "genre_id", "explicit", "acousticness", "energy", "loudness", "valence")

#Group by decade
df_decades = df.filter(df_necessary_columns["year_of_release"] >= 1900)
df_decades = df_decades.withColumn("decade", (col("year_of_release") / 10).cast("int") * 10)

#Drop release_date
df_decades = df_decades.drop("year_of_release")

#Filter out nulls
df_decades = df_decades.filter(df_decades["decade"].isNotNull())

#Filter the genre
df_main_genre = df_decades.withColumn("main_genre_arr",sentences(col("genre_id")))
df_main_genre = df_main_genre.withColumn("main_genre",element_at(col("main_genre_arr"),-1))
df_main_genre = df_main_genre.withColumn("main_genre",element_at(col("main_genre"),-1))
df_pop = df_main_genre.filter(df_main_genre["main_genre"] == "pop")

#Grouping
df_decade_group = df_pop.groupBy("decade").agg(expr("avg(explicit)"),expr("avg(acousticness)"), expr("avg(energy)"), expr("avg(loudness)"),expr("avg(valence)"))

#Sorting
df_final = df_decade_group.sort("decade")

#Save
df_final.write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("file:///home/hadoop/decade_pop")

################# VERSION SQL ###################

#Create table
spark.sql("CREATE TABLE spotify_csv ( track_name STRING, duration BIGINT, explicit STRING, popularity INT,acousticness DOUBLE, danceability DOUBLE, energy DOUBLE, instrumentalness DOUBLE, key INT,liveness DOUBLE,loudness DOUBLE,speechiness DOUBLE,tempo DOUBLE,time_signature INT,valence DOUBLE,album_name STRING,album_type STRING,album_popularity INT,artist_name STRING,artist_popularity INT,followers INT,genre_id STRING,year_of_release INT,month_of_release INT,weekday_of_release INT,day_of_release INT) USING csv OPTIONS (header true, path 'file:///home/hadoop/cleaned_tracks_sample.csv')")

#Select necessary columns where year higher than 1900
spark.sql("CREATE TABLE spotify_necessary_columns AS SELECT year_of_release, genre_id, explicit, acousticness, energy, loudness, valence FROM spotify_csv WHERE year_of_release >= 1900 ")

#Specify decade
spark.sql("CREATE TABLE spotify_decades AS SELECT (CAST(year_of_release / 10 AS INT) * 10) AS decade, genre_id, explicit, acousticness, energy, loudness, valence FROM spotify_necessary_columns")

#Filter by pop genre
spark.sql("CREATE TABLE spotify_genres_split AS SELECT decade, sentences(genre_id) AS main_genre_arr, explicit, acousticness, energy, loudness, valence FROM spotify_decades")
spark.sql("CREATE TABLE spotify_genres_divide AS SELECT decade, element_at(element_at(main_genre_arr,-1),-1) AS main_genre, explicit, acousticness, energy, loudness, valence FROM spotify_genres_split")
spark.sql("CREATE TABLE spotify_pop AS SELECT decade, explicit, acousticness, energy, loudness, valence FROM spotify_genres_divide WHERE main_genre == 'pop'")

#Group by decade
spark.sql("CREATE TABLE spotify_pop_by decade AS SELECT avg(explicit),avg(acousticness), avg(energy), avg(loudness), avg(valence) FROM spotify_pop GROUP BY decade SORT BY decade")