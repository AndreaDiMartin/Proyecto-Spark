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


df.createOrReplaceTempView("AlbumsPerMonth")

albums_per_day = spark.sql(""" 
                            SELECT 
                                year_of_release, album_type, COUNT(DISTINCT artist_name, album_name) AS number_of_albums
                            FROM AlbumsPerMonth
                            GROUP BY year_of_release, album_type
                            ORDER BY year_of_release, album_type
                            """)
                    

albums_per_day.show()

albums_per_day.\
         write\
        .format("json")\
        .mode("overwrite")\
        .save("file:///home/hadoop/Proyecto-Spark/AlbumAnalysis/AlbumTypes/output_sql.json")


spark.stop()
