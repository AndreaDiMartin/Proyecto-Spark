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

summary_albums_per_month = df.groupBy("year_of_release")\
                            .pivot("month_of_release", list(range(1, 13)))\
                            .agg(count_distinct(col("artist_name"), col("album_name"))\
                            .alias("number_of_albums"))\
                            .fillna(0)\
                            .orderBy("year_of_release")

summary_albums_per_month.show()


summary_albums_per_month.\
         write\
        .format("json")\
        .mode("overwrite")\
        .save("file:///home/hadoop/Proyecto-Spark/AlbumAnalysis/AlbumsPerMonthSummary/output_df.json")

spark.stop()
