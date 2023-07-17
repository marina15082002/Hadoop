# Lancer l'application ASKMOVIE

### Prérequis

###### **_Avoir pyspark d'installé sur votre machine_**
###### **_Avoir le fichier filtered_titles.csv dans un hdfs Hadoop et changer le lien dans le code_**
1. Se rendre sur le chemin suivant : Application.py
2. run le fichier Application.py

### Correction du csv
_Le fichier choisit sur kaggle : https://www.kaggle.com/datasets/victorsoeiro/netflix-tv-shows-and-movies?resource=download avait des problèmes qui rendait l'utilisation de pyspark compliqué, nous avons donc corrigé le problème. Le problème provient de certains caractères de retour à la ligne présent dans quelques descriptions des films et séries, nous avons juste supprimé ce caractère._
1. Code pour modifier le fichier : file.py
2. csv d'origine : titles.csv
3. csv corrigé : filtered_titles.csv

### Test de quelques commandes pyspark
1. Se rendre sur le chemin suivant : main.py

### PowerPoint de présentation
1. Se rendre sur le chemin suivant : Ppt_EcoHadoop.pptx

### Statistiques
1. Se rendre sur le chemin suivant : stats.py
_La dernière stats peut produire une erreur_