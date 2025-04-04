from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear la sesión de Spark
spark = SparkSession.builder.appName("MostPopularYearSQL").getOrCreate()

# Definir rutas
input_path = "file:///C:/Users/Daniela/Downloads/cleaned_tracks.csv"

output_path = "file:///C:/Users/Daniela/Downloads/output_most_popular_year_sql"

# Cargar datos desde CSV
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# Asegurar tipos correctos
df = df.withColumn("year_of_release", col("year_of_release").cast("int"))
df = df.withColumn("popularity", col("popularity").cast("int"))

# Registrar DataFrame como tabla SQL temporal
df.createOrReplaceTempView("spotify_data")

# Ejecutar consulta SQL
query = """
    SELECT year_of_release, COUNT(*) AS song_count
    FROM spotify_data
    WHERE popularity >= 50 AND year_of_release IS NOT NULL AND year_of_release > 1
    GROUP BY year_of_release
    ORDER BY year_of_release
"""
result_df = spark.sql(query)

# Mostrar los resultados
result_df.show()

# Guardar la salida en CSV
result_df.write.option("header", True).mode("overwrite").csv(output_path)

print("Trabajo completado con SQL.")

# Detener Spark
spark.stop()

