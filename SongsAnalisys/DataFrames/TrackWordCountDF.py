from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, explode, split, count

# Crear la sesión de Spark
spark = SparkSession.builder.appName("TrackWordCountSpark").getOrCreate()

# Definir rutas de entrada y salida
input_path = "file:///home/hadoop/cleaned_tracks.csv"

output_path = "file:///home/hadoop/output_track_word_count_df"

# Lista de palabras a ignorar (stop words)
STOP_WORDS = set([
    "i", "you", "your", "she", "her", "he", "his", "they", "their", "we", "our", "it", "is", "are", "the", "a", "my",
    "me", "us", "them", "that", "this", "these", "those", "there", "here", "what", "an", "and", "but", "or", "for", "to",
    "of", "in", "on", "at", "with", "by", "as", "if", "than", "then", "when", "where", "while", "who", "which", "why",
    "how", "not", "no", "yes", "all", "some", "more", "most", "like", "about", "over", "here", "there", "now", "then",
    "yo", "tu", "el", "ella", "nosotros", "ellos", "ellas", "mi", "su", "nuestro", "este", "ese", "aquel", "esto", "eso",
    "a", "de", "en", "con", "por", "para", "sin", "mas", "menos", "como", "que", "cual", "quien", "si", "desde", "hasta",
    "durante", "entre", "tras", "ante", "contra", "hacia", "aunque", "porque", "ya", "live", "concert", "music", "group",
    "feat", "ft", "remix", "edit", "studio", "album", "single", "track", "official", "audio", "op", "act"
])

# Cargar datos desde CSV
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# Limpiar y normalizar los nombres de canciones
df_clean = df.select(
    lower(regexp_replace(col("track_name"), "[^a-zA-Z ]", "")).alias("cleaned_track_name")
).filter(col("cleaned_track_name").isNotNull())

# Tokenizar palabras y eliminar stop words
df_words = df_clean.withColumn("word", explode(split(col("cleaned_track_name"), " "))) \
                   .filter(~col("word").isin(STOP_WORDS))

# Contar las ocurrencias de cada palabra
df_word_count = df_words.groupBy("word").agg(count("*").alias("count"))

# Mostrar los resultados
df_word_count.show()

# Guardar el resultado en CSV
df_word_count.write.option("header", True).mode("overwrite").csv(output_path)

print("Trabajo completado con DataFrames.")

# Detener Spark
spark.stop()

