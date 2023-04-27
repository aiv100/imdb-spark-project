import findspark
from pip import __main__
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

findspark.init()

# ----------------Constants for configuring project tasks-------------------------------------
COUNT_TO_SHOW = 20
TOP_COUNT_REGIONS = 100

TOP_COUNT_EPISODES = 50

TOP_RATING_COUNT = 10
COUNT_GROUP_VIEW = 5
# -------------------------Paths to source files-----------------------------------------------
PATH_TITLE_AKAS_TSV = "Data/title_akas.tsv"
PATH_NAME_BASICS_TSV = "Data/name_basics.tsv"
PATH_TITLE_BASICS_TSV = "Data/title_basics.tsv"
PATH_TITLE_PRINCIPALS_TSV = "Data/title_principals.tsv"
PATH_TITLE_EPISODE_TSV = "Data/title_episode.tsv"
PATH_TITLE_RATINGS_TSV = "Data/title_ratings.tsv"
# ---------------------------------------------------------------------------------------------


class Tasks:
    def __init__(self):
        self.spark_session = (SparkSession.builder
                              .master("local")
                              .appName("task app")
                              .config(conf=SparkConf())
                              .getOrCreate())

        self.schema_tsv_title_akas = t.StructType([t.StructField("titleId", t.StringType(), False),
                                                   t.StructField("ordering", t.IntegerType(), False),
                                                   t.StructField("title", t.StringType(), False),
                                                   t.StructField("region", t.StringType(), False),
                                                   t.StructField("language", t.StringType(), False),
                                                   t.StructField("types", t.StringType(), False),
                                                   t.StructField("attributes", t.StringType(), False),
                                                   t.StructField("isOriginalTitle", t.IntegerType(), False)])

        self.schema_tsv_name_basics = t.StructType([t.StructField("nconst", t.StringType(), False),
                                                    t.StructField("primaryName", t.StringType(), False),
                                                    t.StructField("birthYear", t.IntegerType(), False),
                                                    t.StructField("deathYear", t.IntegerType(), False),
                                                    t.StructField("primaryProfession", t.StringType(), False),
                                                    t.StructField("knownForTitles", t.StringType(), False)])

        self.schema_tsv_title_basics = t.StructType([t.StructField("tconst", t.StringType(), False),
                                                     t.StructField("titleType", t.StringType(), False),
                                                     t.StructField("primaryTitle", t.StringType(), False),
                                                     t.StructField("originalTitle", t.StringType(), False),
                                                     t.StructField("isAdult", t.IntegerType(), False),
                                                     t.StructField("startYear", t.IntegerType(), False),
                                                     t.StructField("endYear", t.IntegerType(), False),
                                                     t.StructField("runtimeMinutes", t.IntegerType(), False),
                                                     t.StructField("genres", t.StringType(), False)])

        self.schema_tsv_title_principals = t.StructType([t.StructField("tconst", t.StringType(), False),
                                                         t.StructField("ordering", t.IntegerType(), False),
                                                         t.StructField("nconst", t.StringType(), False),
                                                         t.StructField("category", t.StringType(), False),
                                                         t.StructField("job", t.StringType(), False),
                                                         t.StructField("characters", t.StringType(), False)])

        self.schema_tsv_title_episode = t.StructType([t.StructField("tconst", t.StringType(), False),
                                                      t.StructField("parentTconst", t.StringType(), False),
                                                      t.StructField("seasonNumber", t.IntegerType(), False),
                                                      t.StructField("episodeNumber", t.IntegerType(), False)])

        self.schema_tsv_title_ratings = t.StructType([t.StructField("tconst", t.StringType(), False),
                                                      t.StructField("averageRating", t.DoubleType(), False),
                                                      t.StructField("numVotes", t.IntegerType(), False)])

    # 1. Get all titles of series/movies etc. that are available in Ukrainian
    def show_task1(self):
        df_task1 = self.spark_session.read.csv(PATH_TITLE_AKAS_TSV, schema=self.schema_tsv_title_akas,
                                               header=True, sep="\t")
        df_task1 = df_task1.select("title", "region").where(f.col("region") == "UA")
        df_task1.show(COUNT_TO_SHOW)
        # print("I'm writing ...")
        # df_task1.write.csv("Results/task1")

    # 2. Get the list of people’s names, who were born in the 19th century
    def show_task2(self):
        df_task2 = self.spark_session.read.csv(PATH_NAME_BASICS_TSV, schema=self.schema_tsv_name_basics,
                                               header=True, sep="\t")
        df_task2 = df_task2.select("primaryName", "birthYear")
        df_task2 = df_task2.na.fill(0)
        df_task2 = df_task2.where((f.col("birthYear") >= 1800) & (f.col("birthYear") < 1900))
        df_task2.show(COUNT_TO_SHOW)

    # 3. Get titles of all movies that last more than 2 hours
    def show_task3(self):
        df_task3 = self.spark_session.read.csv(PATH_TITLE_BASICS_TSV, schema=self.schema_tsv_title_basics,
                                               header=True, sep="\t")
        df_task3 = df_task3.select("primaryTitle", "titleType", "runtimeMinutes")
        df_task3 = df_task3.na.fill(0)
        df_task3 = df_task3.where((f.col("runtimeMinutes") > 120) & (f.col("titleType") == "movie"))
        df_task3.show(COUNT_TO_SHOW)

    # 4. Get names of people, corresponding movies/series and characters they played in those films
    def show_task4(self):
        df_actors = self.spark_session.read.csv(PATH_NAME_BASICS_TSV, schema=self.schema_tsv_name_basics,
                                                header=True, sep="\t")
        df_films = self.spark_session.read.csv(PATH_TITLE_BASICS_TSV, schema=self.schema_tsv_title_basics,
                                               header=True, sep="\t")
        df_actors_films = self.spark_session.read.csv(PATH_TITLE_PRINCIPALS_TSV,
                                                      schema=self.schema_tsv_title_principals, header=True, sep="\t")
        df_actors = df_actors.select("primaryName", "nconst")
        df_actors_films = df_actors_films.drop("ordering", "job")
        df_films = df_films.select("tconst", "primaryTitle")
        df_actors = df_actors.join(df_actors_films, df_actors.nconst == df_actors_films.nconst, "inner")
        df_actors = df_actors.filter(df_actors.category == "actor")
        df_actors = df_actors.join(df_films, df_actors.tconst == df_films.tconst, "inner")
        df_actors = df_actors.drop("nconst", "tconst")
        df_actors.show(COUNT_TO_SHOW)

    # 5. Get information about how many adult movies/series etc. there are perregion. Get the top 100 of them from the
    # region with the biggest count tithe region with the smallest one
    def show_task5(self):
        df_task5 = self.spark_session.read.csv(PATH_TITLE_AKAS_TSV, schema=self.schema_tsv_title_akas,
                                               header=True, sep="\t")
        df_task5 = df_task5.select("title", "region")
        df_task5 = df_task5.groupBy("region").count()
        df_task5 = df_task5.orderBy(f.col("count")).limit(TOP_COUNT_REGIONS)
        print("Top 100 of them from the region with the smallest count")
        df_task5.show(TOP_COUNT_REGIONS)
        df_task5 = df_task5.orderBy(f.col("count").desc()).limit(TOP_COUNT_REGIONS)
        print("Top 100 of them from the region with the biggest count")
        df_task5.show(TOP_COUNT_REGIONS)

    # 6. Get information about how many episodes in each TV Series.Get the top50 of them starting from the TV Series
    # with the biggest quantity of episodes
    def show_task6(self):
        df_films = self.spark_session.read.csv(PATH_TITLE_BASICS_TSV, schema=self.schema_tsv_title_basics,
                                               header=True, sep="\t")
        df_episodes = self.spark_session.read.csv(PATH_TITLE_EPISODE_TSV, schema=self.schema_tsv_title_episode,
                                                  header=True, sep="\t")
        df_films = df_films.select("tconst", "primaryTitle")
        df_episodes = df_episodes.select("tconst", "episodeNumber")
        df_films = df_films.join(df_episodes, df_films.tconst == df_episodes.tconst, "inner")
        df_films = df_films.drop("tconst").orderBy(f.col("episodeNumber").desc()).limit(TOP_COUNT_EPISODES)
        df_films.show(TOP_COUNT_EPISODES)

    # 7. Get 10 titles of the most popular movies/series etc. by each decade
    def show_task7(self):
        df_films = self.spark_session.read.csv(PATH_TITLE_BASICS_TSV, schema=self.schema_tsv_title_basics,
                                               header=True, sep="\t")
        df_films = df_films.na.fill(0).filter((f.col("startYear") != 0) & (f.col("endYear") != 0))
        df_films = df_films.withColumn("decade", ((df_films.endYear - df_films.startYear) / 10 + 1).cast("int"))
        df_ratings = self.spark_session.read.csv(PATH_TITLE_RATINGS_TSV, schema=self.schema_tsv_title_ratings,
                                                 header=True, sep="\t")
        df_films = df_films.join(df_ratings, df_films.tconst == df_ratings.tconst, "inner")
        df_films = df_films.select("primaryTitle", "decade", "averageRating")
        window_dept = Window.partitionBy("decade").orderBy(f.col("averageRating").desc())
        df_films = df_films.withColumn("top", f.row_number().over(window_dept))
        df_films = df_films.filter(f.col("top") <= TOP_RATING_COUNT)
        df_films.show(COUNT_GROUP_VIEW * TOP_RATING_COUNT)

    # 8. Get 10 titles of the most popular movies/series etc. by each genre
    def show_task8(self):
        df_films = self.spark_session.read.csv(PATH_TITLE_BASICS_TSV, schema=self.schema_tsv_title_basics,
                                               header=True, sep="\t")
        df_ratings = self.spark_session.read.csv(PATH_TITLE_RATINGS_TSV, schema=self.schema_tsv_title_ratings,
                                                 header=True, sep="\t")
        df_films = df_films.join(df_ratings, df_films.tconst == df_ratings.tconst, "inner")
        df_films = df_films.select("primaryTitle", "genres", "averageRating")
        df_films = df_films.filter(df_films.genres != "null")
        window_dept = Window.partitionBy("genres").orderBy(f.col("averageRating").desc())
        df_films = df_films.withColumn("top", f.row_number().over(window_dept))
        df_films = df_films.filter(f.col("top") <= TOP_RATING_COUNT)
        df_films.show(COUNT_GROUP_VIEW * TOP_RATING_COUNT)


# To test the operation of methods of this class, uncomment the desired task and run it
if __name__ == __main__:
    tasks = Tasks()
    # tasks.show_task1()
    # tasks.show_task2()
    # tasks.show_task3()
    # tasks.show_task4()
    # tasks.show_task5()
    # tasks.show_task6()
    # tasks.show_task7()
    # tasks.show_task8()
