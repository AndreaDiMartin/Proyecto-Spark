from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, count_distinct

spark = SparkSession.builder.master("local[*]").getOrCreate()


schema = StructType(
    [
        StructField("album_name", StringType(), True),
        StructField("album_type", StringType(), True),
        StructField("album_popularity", IntegerType(), True),
        StructField("artist_name", StringType(), True),
        StructField("year_of_release", IntegerType(), True),
        StructField("month_of_release", IntegerType(), True),
        StructField("weekday_of_release", IntegerType(), True),
        StructField("day_of_release", IntegerType(), True)
    ]
)


# read data
df = spark\
        .read\
        .schema(schema)\
        .option("header","true")\
        .csv("file:///home/hadoop/Proyecto-Spark/AlbumAnalysis/album_data.csv")

albums_per_year = df.groupBy("year_of_release")\
                    .agg(count_distinct(col("artist_name"), col("album_name"))\
                    .alias("number_of_albums"))\
                    .sort("year_of_release")
                    

albums_per_year.fillna({"year_of_release": "Unknown"}).show()

albums_per_year.\
         write\
        .format("json")\
        .mode("overwrite")\
        .save("file:///home/hadoop/Proyecto-Spark/AlbumAnalysis/AlbumsPerYear/output_df.json")

spark.stop()
