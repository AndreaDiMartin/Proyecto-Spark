from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder.appName("SongsFeaturesPerYear_SQL").getOrCreate()

# Definir ruta de entrada
input_path = "file:///home/hadoop/cleaned_tracks.csv"

output_path = "file:///home/hadoop/output_feature_per_year_sql"

# Cargar datos en DataFrame con esquema y crear tabla temporal
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
df.createOrReplaceTempView("songs")

# Ejecutar consulta SQL para calcular promedios
query = """
    SELECT 
        year_of_release,
        AVG(explicit) AS avg_explicit,
        AVG(acousticness) AS avg_acousticness,
        AVG(danceability) AS avg_danceability,
        AVG(energy) AS avg_energy,
        AVG(instrumentalness) AS avg_instrumentalness,
        AVG(liveness) AS avg_liveness,
        AVG(loudness) AS avg_loudness,
        AVG(speechiness) AS avg_speechiness,
        AVG(tempo) AS avg_tempo,
        AVG(valence) AS avg_valence
    FROM songs
    WHERE year_of_release IS NOT NULL AND popularity > 70
    GROUP BY year_of_release
    ORDER BY year_of_release
"""

# Ejecutar la consulta y obtener el resultado
avg_features_sql = spark.sql(query)

# Mostrar resultados en consola
avg_features_sql.show()

# Guardar en CSV
avg_features_sql.write.mode("overwrite").option("header", "true").csv(output_path)

# Cerrar sesión de Spark
spark.stop()