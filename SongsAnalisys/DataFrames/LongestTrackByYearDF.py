from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, regexp_replace, lower, max as spark_max

# Crear la sesión de Spark
spark = SparkSession.builder.appName("LongestTrackByYear").getOrCreate()

# Definir rutas
input_path = "file:///home/hadoop/cleaned_tracks.csv"

output_path = "file:///home/hadoop/output_longest_track_by_year_df"

# Cargar el dataset desde un CSV
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

# Convertir los tipos de datos adecuados
df = df.withColumn("year_of_release", col("year_of_release").cast("int"))
df = df.withColumn("track_name", lower(regexp_replace(col("track_name"), "[^a-zA-Z0-9 ]", "")))

# Filtrar valores nulos
df = df.filter(col("year_of_release").isNotNull())

# Calcular la longitud del nombre de la canción
df = df.withColumn("track_name_length", length(col("track_name")))

# Encontrar la longitud máxima de un nombre de canción por año
result_df = df.groupBy("year_of_release").agg(spark_max("track_name_length").alias("max_length"))

# Mostrar los resultados
result_df.show()

# Guardar en CSV
result_df.write.mode("overwrite").option("header", "true").csv(output_path)

# Cerrar sesión de Spark
spark.stop()

