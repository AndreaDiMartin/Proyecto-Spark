from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder.appName("TrackWordCountSQL").getOrCreate()

# Definir rutas de entrada y salida
input_path = "file:///C:/Users/Daniela/Downloads/cleaned_tracks.csv"

output_path = "file:///C:/Users/Daniela/Downloads/output/output_track_word_count_sql"

# Cargar datos desde CSV
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# Registrar DataFrame como tabla SQL temporal
df.createOrReplaceTempView("spotify_data")

# Limpiar y tokenizar palabras
df_clean = spark.sql("""
    SELECT LOWER(REGEXP_REPLACE(track_name, '[^a-zA-Z ]', '')) AS cleaned_track_name
    FROM spotify_data
    WHERE track_name IS NOT NULL
""")
df_clean.createOrReplaceTempView("cleaned_data")

# Consulta SQL para contar palabras sin stop words
query = """
    SELECT word, COUNT(*) AS count
    FROM (
        SELECT EXPLODE(SPLIT(cleaned_track_name, ' ')) AS word FROM cleaned_data
    )
    WHERE word NOT IN (
        'i', 'you', 'your', 'she', 'her', 'he', 'his', 'they', 'their', 'we', 'our', 'it', 'is', 'are', 'the', 'a', 'my',
        'me', 'us', 'them', 'that', 'this', 'these', 'those', 'there', 'here', 'what', 'an', 'and', 'but', 'or', 'for', 'to',
        'of', 'in', 'on', 'at', 'with', 'by', 'as', 'if', 'than', 'then', 'when', 'where', 'while', 'who', 'which', 'why',
        'how', 'not', 'no', 'yes', 'all', 'some', 'more', 'most', 'like', 'about', 'over', 'here', 'there', 'now', 'then',
        'yo', 'tu', 'el', 'ella', 'nosotros', 'ellos', 'ellas', 'mi', 'su', 'nuestro', 'este', 'ese', 'aquel', 'esto', 'eso',
        'a', 'de', 'en', 'con', 'por', 'para', 'sin', 'mas', 'menos', 'como', 'que', 'cual', 'quien', 'si', 'desde', 'hasta',
        'durante', 'entre', 'tras', 'ante', 'contra', 'hacia', 'aunque', 'porque', 'ya', 'live', 'concert', 'music', 'group',
        'feat', 'ft', 'remix', 'edit', 'studio', 'album', 'single', 'track', 'official', 'audio', 'op', 'act'
    )
    GROUP BY word
    ORDER BY count DESC
"""
df_word_count = spark.sql(query)

# Mostrar los resultados
df_word_count.show()

# Guardar el resultado en CSV
df_word_count.write.option("header", True).mode("overwrite").csv(output_path)

print("Trabajo completado con SQL.")

# Detener Spark
spark.stop()

