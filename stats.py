from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, StringType


spark = SparkSession.builder \
        .appName("Statistiques sur Hadoop") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

df = spark.read.csv("hdfs://localhost:9000/titles.csv", header=True, inferSchema=True)

# Initialiser SparkSession
spark = SparkSession.builder.appName("TendancesTemporelles").getOrCreate()

# Convertir la colonne 'release_year' en type 'date'
df = df.withColumn("release_year", df["release_year"].cast("date"))

# Trier le DataFrame par année de sortie
df = df.orderBy("release_year")

# Agréger les données : Calculer le nombre de films par année de sortie
films_par_annee = df.groupBy("release_year").count()

# Afficher les données agrégées
films_par_annee.show()

# Sauvegarder les données agrégées dans un fichier CSV avec Spark en utilisant une sorti avec l'HDFS
output_hdfs_path = "hdfs://localhost:9000/tendances_temporelles.csv"
films_par_annee.write.mode("overwrite").option("header", "true").csv(output_hdfs_path)

spark.stop()

spark = SparkSession.builder.appName("MoyenneScores").getOrCreate()

# Charger le fichier CSV dans un DataFrame Spark
df = spark.read.csv("hdfs://localhost:9000/titles.csv", header=True, inferSchema=True)

# Convertir la colonne "imdb_score" en type numérique
df = df.withColumn("imdb_score", df["imdb_score"].cast("double"))

# Effectuer l'agrégation en calculant la moyenne du score IMDB par type de film
moyenne_scores = df.groupBy("type").avg("imdb_score")

# Afficher les résultats
moyenne_scores.show()

# Définir le chemin de sortie sur HDFS
output_hdfs_path = "hdfs://localhost:9000/moyenne_scores_par_type.csv"
# Sauvegarder les résultats dans un fichier CSV sur HDFS
moyenne_scores.write.mode("overwrite").option("header", "true").csv(output_hdfs_path)

# Arrêter SparkSession
spark.stop()

spark = SparkSession.builder.appName("GenreCounts").getOrCreate()

# Charger le fichier CSV dans un DataFrame Spark
df = spark.read.csv("hdfs://localhost:9000/titles.csv", header=True, inferSchema=True)

parse_genres = udf(lambda genres_str: genres_str.strip("[]").replace("'", "").split(", ") if genres_str.startswith("[") else [genres_str], ArrayType(StringType()))
df = df.withColumn("genres", parse_genres(df["genres"]))

# Utiliser la méthode explode pour créer de nouvelles lignes pour chaque genre
df_exploded = df.select("id", "title", "type", "description", "release_year", "age_certification",
                        "runtime", "imdb_id", "imdb_score", "imdb_votes", "tmdb_popularity",
                        "tmdb_score", explode("genres").alias("Genre"))

# Effectuer l'agrégation en calculant le nombre de films pour chaque genre
genre_counts = df_exploded.groupBy("Genre").count()

# Afficher les résultats
genre_counts.show()

# Définir le chemin de sortie sur HDFS
output_hdfs_path = "hdfs://localhost:9000/genre_counts.csv"

# Sauvegarder les résultats dans un fichier CSV sur HDFS
genre_counts.write.mode("overwrite").option("header", "true").csv(output_hdfs_path)

# Arrêter SparkSession
spark.stop()