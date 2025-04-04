from pyspark.sql import SparkSession 

# Crear sesión de Spark
spark = SparkSession.builder.appName("LongestTrackByYearSQL").getOrCreate()

# Definir rutas
input_path = "file:///home/hadoop/cleaned_tracks.csv"

output_path = "file:///home/hadoop/output_longest_track_by_year_sql"

# Leer datos en un DataFrame
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

# Registrar DataFrame como tabla SQL temporal
df.createOrReplaceTempView("spotify_tracks")

# Consulta SQL para obtener la longitud máxima del nombre de la canción por año
query = """
SELECT year_of_release, MAX(LENGTH(track_name)) AS max_length
FROM spotify_tracks
WHERE year_of_release IS NOT NULL
GROUP BY year_of_release
ORDER BY year_of_release
"""

# Ejecutar la consulta
result_sql = spark.sql(query)

# Mostrar los resultados
result_sql.show()

# Guardar en CSV
result_sql.write.mode("overwrite").option("header", "true").csv(output_path)

# Cerrar sesión de Spark
spark.stop()

