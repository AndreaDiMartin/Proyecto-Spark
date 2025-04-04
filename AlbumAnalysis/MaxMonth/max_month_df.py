from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import max, count, collect_list, col


spark = SparkSession.builder.master("local[*]").getOrCreate()


schema = StructType(
    [
        StructField("year_of_release", IntegerType(), True),
        StructField("month_of_release", IntegerType(), True),
        StructField("number_of_albums", IntegerType(), True)
    ]
)

df = spark\
        .read\
        .schema(schema)\
        .json("file:///home/hadoop/Proyecto-Spark/AlbumAnalysis/AlbumsPerMonth/output_df.json")



# Calcular el mes con mas lanzamintos de cada anio (Incluyendo diciembre) 
max_albums_per_year = df.groupBy("year_of_release")\
                        .agg(max("number_of_albums").alias("max_albums"))\
                        .sort("year_of_release")


df_join = (df.alias('df')).join(max_albums_per_year.alias('m'), 
                      (col("df.year_of_release") == col("m.year_of_release")) & 
                      (col("df.number_of_albums") == col("m.max_albums")), 
                      "inner").select(col("df.year_of_release"), col("df.month_of_release"), col("m.max_albums"))

df_join.show()

df_join.\
         write\
        .format("json")\
        .mode("overwrite")\
        .save("file:///home/hadoop/Proyecto-Spark/AlbumAnalysis/MaxMonth/output_df.json")


# Calcular cuales son los meses con mas lanzamientos y su lista de anios
month_and_year_list = df_join.groupBy("month_of_release")\
                            .agg(count("year_of_release").alias("num_years"),
                                collect_list("year_of_release").alias("years_list"))\
                            .orderBy("month_of_release")

month_and_year_list.show()

month_and_year_list.\
         write\
        .format("json")\
        .mode("overwrite")\
        .save("file:///home/hadoop/Proyecto-Spark/AlbumAnalysis/MaxMonth/output_mly_df.json")


# Mismo calculo anterior, pero sin considerar el mes de diciembre
max_albums_wd = df.filter(df.month_of_release != 12)

max_albums_per_year_wd = max_albums_wd.groupBy("year_of_release")\
                        .agg(max("number_of_albums").alias("max_albums"))\
                        .sort("year_of_release")


df_join_wd = (max_albums_wd.alias('dfwd')).join(max_albums_per_year_wd.alias('mwd'), 
                      (col("dfwd.year_of_release") == col("mwd.year_of_release")) & 
                      (col("dfwd.number_of_albums") == col("mwd.max_albums")), 
                      "inner").select(col("dfwd.year_of_release"), col("dfwd.month_of_release"), col("mwd.max_albums"))


month_and_year_list_wd = df_join_wd.groupBy("month_of_release")\
                            .agg(count("year_of_release").alias("num_years"),
                                collect_list("year_of_release").alias("years_list"))\
                            .orderBy("month_of_release")

month_and_year_list_wd.show()

month_and_year_list_wd.\
         write\
        .format("json")\
        .mode("overwrite")\
        .save("file:///home/hadoop/Proyecto-Spark/AlbumAnalysis/MaxMonth/output_mlywd_df.json")

spark.stop()
