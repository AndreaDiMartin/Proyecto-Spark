from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Crear la sesión de Spark
spark = SparkSession.builder.appName("MostPopularYearSpark").getOrCreate()

# Definir rutas de entrada y salida
input_path = "file:///C:/Users/Daniela/Downloads/cleaned_tracks.csv"
output_path = "file:///C:/Users/Daniela/Downloads/output_most_popular_year_df"

# Cargar datos desde CSV
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# Asegurar tipos correctos
df = df.withColumn("year_of_release", col("year_of_release").cast("int"))
df = df.withColumn("popularity", col("popularity").cast("int"))

# Filtrar datos válidos
filtered_df = df.filter(
    (col("popularity") >= 50) & 
    (col("year_of_release").isNotNull()) & 
    (col("year_of_release") > 1)
)

# Contar las canciones populares por año
result_df = filtered_df.groupBy("year_of_release").agg(count("*").alias("song_count"))

# Mostrar los resultados
result_df.show()

# Guardar la salida en CSV
result_df.write.option("header", True).mode("overwrite").csv(output_path)

print("Trabajo completado con DataFrames.")

# Detener Spark
spark.stop()


