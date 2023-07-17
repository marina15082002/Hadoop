from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import re
from pyspark.sql.functions import avg, count, sum, explode, split, regexp_replace, col, trim


# Créer une instance SparkSession
spark = SparkSession.builder \
    .appName("Statistiques sur Hadoop") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()


# Lire le fichier de données à partir de Hadoop HDFS
data = spark.read.csv("hdfs://localhost:9000/titles.csv", header=True, inferSchema=True)


# Calculer le nombre total de lignes
total_count = data.count()
print("Nombre total de lignes : ", total_count)

# Calculer la moyenne des scores IMDb
imdb_score = data.withColumn("imdb_score", data["imdb_score"].cast("double"))
avg_imdb_score = imdb_score.select(avg("imdb_score")).first()[0]
print("Moyenne des scores IMDb :", avg_imdb_score)

# Compter le nombre d'éléments par type (nb films et nb series)
count_by_type = data.groupBy("type").count()
count_by_type.show()

# Convertir la colonne "runtime" en type numérique
runtime = data.withColumn("runtime", data["runtime"].cast("integer"))

# Calculer la durée totale des films
total_runtime = runtime.select(sum("runtime")).first()[0]
print("Durée totale des films :", total_runtime)

# Calculer la moyenne des scores IMDb pour chaque genre (celui est faux ne prend pas en compte les vrai genres)
avg_imdb_by_genre = imdb_score.groupBy("genres").agg(avg("imdb_score").alias("avg_imdb_score"))
avg_imdb_by_genre.show()


# récupérer le numéro de colonne de la colonne "genres"
genres_index = data.columns.index("genres")



# Compter le nombre de films pour chaque filme qui a le genre "Action"
count_by_genre = data.filter(data["genres"].contains("action"))
count_by_genre.show()



# récupérer la liste des genres
list_genres = data.select("genres").distinct().collect()
list_genres = [row["genres"] for row in list_genres]

# On met les genres dans un dictionnaire
genre_dict = {}

for i in range(len(list_genres)):
    # si list_genres[i] est un tableau
    if type(list_genres[i]) == str and list_genres[i].startswith("[") and list_genres[i].endswith("]"):
        list_genre = list_genres[i].split()
    if type(list_genre) == list:
        for j in range(len(list_genre)):
            # supprimer les guillements, les espaces et les virgules
            list_genre[j] = list_genre[j].replace("'", "").replace(",", "").replace(" ", "").replace("[", "").replace("]", "")
            genre_dict[list_genre[j]] = 0
print(genre_dict)

# on compte le nombre d'élément qui contiennent les genres
genre = data.select("genres")
for key in genre_dict:
    count_by_genre = genre.filter(genre["genres"].contains(key)).count()
    genre_dict[key] = str(count_by_genre)
print(genre_dict)

# mettre imdb_genre à vide pour pouvoir le remplir
spark2 = SparkSession.builder.getOrCreate()
# récupérer la première clé de genre_dict
first_key = list(genre_dict.keys())[0]
# dataframe sous la forme (genre, avg_imdb_score)

# split , [ ] et ' pour avoir un tableau de genre sur la meme ligne de code
#transformer une string de forme "['crime', 'drama', 'action']" en tableau
"""
select = data.select("genres".split()[0]).explode("genres")
select.show()
avg_scores_df = data.select(data.select("genre").split().explode("genres").alias("genre"), col("imdb_score").cast("float")).groupBy("genre").agg(avg("imdb_score").alias("avg_score"))
avg_scores_df.show()

imdb_genre = data.groupBy("genres").agg(avg("imdb_score"))
imdb_genre = imdb_genre.filter(genre["genres"].contains(first_key)).agg(avg("imdb_score"))
# afficher le résultat en deux colonnes (genre, avg_imdb_score)
imdb_genre.show()
for key in genre_dict:
    # remplir imdb_genre avec les données
    imdb_genre.union(data.filter(genre["genres"].contains(key)).agg(avg("imdb_score")))
imdb_genre.show()
"""

avg_imdb = {}
for key in genre_dict:
    genre = data.groupBy("genres").agg(avg("imdb_score"))
    count_by_genre = genre.filter(genre["genres"].contains(key))
    avg_imdb[key] = str(count_by_genre)
action_avg = avg_imdb["action"]
print(avg_imdb)
print(action_avg)

data = data.withColumn("imdb_score", col("imdb_score").cast("decimal(10,2)"))

# Calcul de la moyenne des scores IMDB pour le genre "Action"
action_avg = data.filter(data["genres"] == "action").selectExpr("mean(imdb_score)").first()[0]
print("Moyenne des scores IMDB pour le genre Action :", action_avg)


# Fermer la session Spark
spark.stop()