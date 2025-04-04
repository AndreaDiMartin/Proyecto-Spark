from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Crear sesión de Spark
spark = SparkSession.builder.appName("SongsFeaturesPerYear_DF").getOrCreate()

# Definir ruta de entrada
input_path = "file:///home/hadoop/cleaned_tracks.csv"

output_path = "file:///home/hadoop/outputsongs_features_per_year"

# Cargar datos en DataFrame
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

# Filtrar datos válidos
df = df.filter((col("year_of_release").isNotNull()) & (col("popularity") > 70))

# Seleccionar columnas necesarias con conversión de tipos
df = df.select(
    col("year_of_release").cast("int"),
    col("explicit").cast("int"),
    col("acousticness").cast("float"),
    col("danceability").cast("float"),
    col("energy").cast("float"),
    col("instrumentalness").cast("float"),
    col("liveness").cast("float"),
    col("loudness").cast("float"),
    col("speechiness").cast("float"),
    col("tempo").cast("float"),
    col("valence").cast("float")
)

# Calcular el promedio de características por año
avg_features_df = df.groupBy("year_of_release").agg(
    avg("explicit").alias("avg_explicit"),
    avg("acousticness").alias("avg_acousticness"),
    avg("danceability").alias("avg_danceability"),
    avg("energy").alias("avg_energy"),
    avg("instrumentalness").alias("avg_instrumentalness"),
    avg("liveness").alias("avg_liveness"),
    avg("loudness").alias("avg_loudness"),
    avg("speechiness").alias("avg_speechiness"),
    avg("tempo").alias("avg_tempo"),
    avg("valence").alias("avg_valence")
).orderBy("year_of_release")

# Mostrar resultados en consola
avg_features_df.show()

# Guardar en CSV
avg_features_df.write.mode("overwrite").option("header", "true").csv(output_path)

# Cerrar sesión de Spark
spark.stop()

