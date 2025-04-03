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


df.createOrReplaceTempView("AlbumsData")

albums_per_month = spark.sql(""" 
                            SELECT 
                                year_of_release, month_of_release, COUNT(DISTINCT artist_name, album_name) AS number_of_albums
                            FROM AlbumsData
                            GROUP BY year_of_release, month_of_release
                            """)


albums_per_month.createOrReplaceTempView("AlbumsPerMonth")

summary_albums_per_month = spark.sql("""
                                    SELECT *
                                    FROM AlbumsPerMonth
                                    PIVOT (
                                        SUM(number_of_albums)
                                        FOR month_of_release IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
                                    )
                                    ORDER BY year_of_release
                                    """)

summary_albums_per_month.createOrReplaceTempView("SummaryAlbumsPerMonth")

summary_albums_per_month = spark.sql("""
SELECT 
    year_of_release, 
    COALESCE(`1`, 0) AS `1`,
    COALESCE(`2`, 0) AS `2`,
    COALESCE(`3`, 0) AS `3`,
    COALESCE(`4`, 0) AS `4`,
    COALESCE(`5`, 0) AS `5`,
    COALESCE(`6`, 0) AS `6`,
    COALESCE(`7`, 0) AS `7`,
    COALESCE(`8`, 0) AS `8`,
    COALESCE(`9`, 0) AS `9`,
    COALESCE(`10`, 0) AS `10`,
    COALESCE(`11`, 0) AS `11`,
    COALESCE(`12`, 0) AS `12`
FROM SummaryAlbumsPerMonth;
""")

summary_albums_per_month.show()


summary_albums_per_month.\
         write\
        .format("json")\
        .mode("overwrite")\
        .save("file:///home/hadoop/Proyecto-Spark/AlbumAnalysis/AlbumsPerMonthSummary/output_sql.json")

spark.stop()
