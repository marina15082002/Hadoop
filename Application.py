from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import re
from pyspark.sql.functions import avg, count, sum, explode, split, regexp_replace, col, trim, udf, lit, size, array, expr, concat_ws, concat, array_intersect
from functools import reduce

class Application:
    age = 18
    type = ""
    genre = []
    period = []
    country = []
    popular = True
    duree = 0
    duree_moyenne = 0
    SHOW = 10

    spark = SparkSession.builder \
        .appName("Statistiques sur Hadoop") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    data = spark.read.csv("hdfs://localhost:9000/titles.csv", header=True, inferSchema=True)

    def FunctAge(self):
        print("What is your age?\n")
        age = input()
        age = int(age)
        self.age = age

        if self.age < 7:
            print("Warning: You are too young to watch movies or series")
            exit()

        if age < 18:
            print("You are not allowed to watch adult movies or series")
            self.menu()
        else:
            self.menu()

    def FunctType(self):
        print("Would you like to watch a movie or a TV series?")
        print("\t1: Movie")
        print("\t2: TV series")
        print("\t3: Any")

        choix = input()

        if choix == "3":
            self.type = ""
            return self.menu()
        else:
            average_duration = 0

            if choix == "1":
                print("You have chosen to watch a movie\n")
                self.type = "MOVIE"
                average_duration = \
                self.data.filter(col("type").contains(self.type)).agg(avg(col("runtime")).cast("int")).collect()[0][0]
                self.average_duration = average_duration
                print("The average duration of a movie is", average_duration, "minutes")
            elif choix == "2":
                print("You have chosen to watch a TV series\n")
                self.type = "SHOW"
                average_duration = \
                self.data.filter(col("type").contains(self.type)).agg(avg(col("runtime")).cast("int")).collect()[0][0]
                self.average_duration = average_duration
                print("The average duration of a TV series is", average_duration, "minutes")
            else:
                print("You have not chosen a valid type of movie or TV series")
                return self.FunctType()

            print("Do you prefer a", self.type, "shorter or longer than the average?")
            print("\t1: Shorter")
            print("\t2: Longer")
            print("\t3: Any")

            choix = input()

            if choix == "1":
                print("You have chosen a movie shorter than the average")
                self.duration = average_duration - 1
            elif choix == "2":
                print("You have chosen a TV series longer than the average")
                self.duration = average_duration + 1
            elif choix == "3":
                self.duration = 0
            else:
                print("You have not chosen a valid duration")
                return self.FunctType()

            return self.menu()

    def FunctGenre(self):
        list_genres = self.data.select("genres").distinct().collect()
        list_genres = [row["genres"] for row in list_genres]

        genre_list = []

        for i in range(len(list_genres)):
            if type(list_genres[i]) == str and list_genres[i].startswith("[") and list_genres[i].endswith("]"):
                list_genre = list_genres[i].replace("[", "").replace("]", "").replace("'", "").replace(" ", "").split(
                    ",")
                genre_list.extend(list_genre)

        genre_list = list(set(genre_list))
        genre_list.sort()

        print("What genre of movie would you like to watch?")
        print("Here are the genres you have chosen:", self.genre)
        print("When you are finished with your selection, type 0")
        choix = "1"
        while choix != "0":
            for i in range(1, len(genre_list)):
                print("\t", i, ":", genre_list[i])

            choix = input()

            if choix != "0":
                if choix.isdigit():
                    choix = int(choix)
                    if 1 <= choix < len(genre_list):
                        if genre_list[choix] not in self.genre:
                            self.genre.append(genre_list[choix])
                        else:
                            confirm = ""
                            while confirm != "1" and confirm != "2":
                                print("You have already chosen this genre. Do you want to remove it?")
                                print("\t1: Yes")
                                print("\t2: No")
                                confirm = input()
                                if confirm == "1":
                                    self.genre.remove(genre_list[choix])
                    else:
                        print("Invalid choice. Please choose a valid number or type 0 to finish.")
                else:
                    print("Invalid input. Please enter a valid number.")

            print("Here are the genres you have chosen:", sorted(self.genre))
            print("Continue your selection or type 0 to finish")

        return self.menu()

    def FunctPeriod(self):
        list_dates = self.data.select(col("release_year").cast("integer")).distinct().filter(col("release_year").isNotNull()).collect()

        year_set = set()
        temp_list_dates = []
        for row in list_dates:
            year = row["release_year"] - row["release_year"] % 10
            if year not in year_set:
                year_set.add(year)
                temp_list_dates.append(year)

        list_dates = sorted(temp_list_dates)

        print("What period would you like to watch?")
        print("Here are the periods you have chosen:", self.period)
        print("When you are finished with your selection, type 0")
        choix = "1"
        while choix != "0":
            for i in range(len(list_dates)):
                print("\t", i + 1, ":", list_dates[i])

            choix = input()

            if choix != "0":
                if choix.isdigit():
                    choix = int(choix)
                    if 1 <= choix <= len(list_dates):
                        if list_dates[choix - 1] not in self.period:
                            self.period.append(list_dates[choix - 1])
                        else:
                            confirm = ""
                            while confirm != "1" and confirm != "2":
                                print("You have already chosen this period. Do you want to remove it?")
                                print("\t1: Yes")
                                print("\t2: No")
                                confirm = input()
                                if confirm == "1":
                                    self.period.remove(list_dates[choix - 1])
                    else:
                        print("Invalid choice. Please choose a valid number or type 0 to finish.")
                else:
                    print("Invalid input. Please enter a valid number.")

            print("Here are the periods you have chosen:", sorted(self.period))
            print("Continue your selection or type 0 to finish")

        return self.menu()

    def FunctCountry(self):
        list_countries = self.data.select("production_countries").distinct().collect()
        list_countries = [row["production_countries"] for row in list_countries]


        countrie_list = []

        for i in range(len(list_countries)):
            if type(list_countries[i]) == str and list_countries[i].startswith("[") and list_countries[i].endswith("]"):
                list_countrie = list_countries[i].replace("[", "").replace("]", "").replace("'", "").replace(" ", "").split(",")
                countrie_list.extend(list_countrie)

        countrie_list = list(set(countrie_list))
        countrie_list.sort()

        list_genres = self.data.select("genres").distinct().collect()
        list_genres = [row["genres"] for row in list_genres]

        genre_list = []

        for i in range(len(list_genres)):
            if type(list_genres[i]) == str and list_genres[i].startswith("[") and list_genres[i].endswith("]"):
                list_genre = list_genres[i].replace("[", "").replace("]", "").replace("'", "").replace(" ", "").split(
                    ",")
                genre_list.extend(list_genre)

        genre_list = list(set(genre_list))
        genre_list.sort()

        country_list = [country for country in countrie_list if country not in genre_list]

        print("Which production country would you like to watch?")
        print("Here are the countries you have chosen:", self.country)
        print("When you are finished with your selection, type 0")
        choix = "1"
        while choix != "0":
            for i in range(1, len(country_list)):
                print("\t", i, ":", country_list[i])

            choix = input()

            if choix != "0":
                if choix.isdigit():
                    choix = int(choix)
                    if 1 <= choix < len(country_list):
                        if country_list[choix] not in self.country:
                            self.country.append(country_list[choix])
                        else:
                            confirm = ""
                            while confirm != "1" and confirm != "2":
                                print("You have already chosen this country. Do you want to remove it?")
                                print("\t1: Yes")
                                print("\t2: No")
                                confirm = input()
                                if confirm == "1":
                                    self.country.remove(country_list[choix])
                    else:
                        print("Invalid choice. Please choose a valid number or type 0 to finish.")

            print("Here are the countries you have chosen:", sorted(self.country))
            print("Continue your selection or type 0 to finish")

        return self.menu()

    def FunctPopular(self):
        confirm = ""
        while confirm != "1" and confirm != "2":
            print("Do you want to watch the most popular movies and TV series?")
            print("\t1: Yes")
            print("\t2: No")
            confirm = input()
            if confirm == "1":
                self.popular = True
            else:
                self.popular = False

        return self.menu()

    def createColAge(self):
        predict = self.data
        predictAge = predict.withColumn("age", when(col("age_certification") == "R", lit(18)).otherwise(lit(19)))
        predictAge = predictAge.withColumn("age", when(col("age_certification") == "TV-MA", lit(18)).otherwise(col("age")))
        predictAge = predictAge.withColumn("age", when(col("age_certification") == "PG-14", lit(14)).otherwise(col("age")))
        predictAge = predictAge.withColumn("age", when(col("age_certification") == "PG-13", lit(13)).otherwise(col("age")))
        predictAge = predictAge.withColumn("age", when(col("age_certification") == "TV-Y", lit(7)).otherwise(col("age")))
        return predictAge

    def FunctEnd(self):
        predict = self.createColAge()

        predict = predict.filter(col("age") <= self.age)

        if self.type != "":
            predict = predict.filter(col("type").contains(self.type))
        if self.duree != 0:
            print("duree : ", self.duree)
            if self.duree > self.duree_moyenne:
                predict = predict.filter(col("runtime") > self.duree)
            else:
                predict = predict.filter(col("runtime") < self.duree)
        if self.period != []:
            for i in range(len(self.period)):
                self.period[i] = int(self.period[i])
                for j in range(9):
                    self.period.append(self.period[i] + j)

            predict = predict.filter(col("release_year").isin(self.period))
        if self.country != []:
            country_filters = [col("production_countries").contains(country) for country in self.country]
            predict = predict.filter(reduce(lambda a, b: a | b, country_filters))
        if self.genre != []:
            # prendre les films qui ont au moins un genre dans la liste
            genre_filters = [col("genres").contains(genre) for genre in self.genre]
            predict = predict.filter(reduce(lambda a, b: a | b, genre_filters))
            # ajouter une colonne qui contient le nombre de genre dans la liste
            predict = predict.withColumn("nb_genre", lit(0))

            for genre in self.genre:
                predict = predict.withColumn("nb_genre", when(col("genres").contains(genre), col("nb_genre") + 1).otherwise(col("nb_genre")))

            predict = predict.orderBy(desc("nb_genre"), desc("imdb_score"))
            predict.select("title", "description").show(10, False)
            exit()
        if self.popular:
            predict = predict.orderBy(desc("imdb_score"))
            predict.select("title", "description").show(10, False)
            exit()
        else:
            predict.select("title", "description").show(10, False)
            exit()

    def FunctQuit(self):
        print("Thank you for using our application!")
        exit()

    def menu(self):
        print("Choose the question you want to answer to get a movie or TV series recommendation\n")
        print("\t1: Set conditions on movies or TV series")
        print("\t2: Choose a genre of movie/TV series")
        print("\t3: Choose a production period of the movie/TV series")
        print("\t4: Choose a production country of the movie/TV series")
        print("\t5: Watch a popular movie/TV series (the default answer is yes)")
        print("\t6: Finish answering the questions")
        print("\tq: Quit the application")

        choix = ""
        while choix != "1" and choix != "2" and choix != "3" and choix != "4" and choix != "5" and choix != "6" and choix != "q":
            choix = input()
            if choix == "1":
                self.FunctType()
            elif choix == "2":
                self.FunctGenre()
            elif choix == "3":
                self.FunctPeriod()
            elif choix == "4":
                self.FunctCountry()
            elif choix == "5":
                self.FunctPopular()
            elif choix == "6":
                self.FunctEnd()
            elif choix == "q":
                self.FunctQuit()
            else:
                print("Invalid choice. Please choose a valid number or type 'q' to quit the application.")

    def start(self):
        print("Welcome to ASKMOVIE\n")

        self.FunctAge()
        self.launched()


    def launched(self):
        self.menu()

app = Application()
app.start()
