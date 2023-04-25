import findspark
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

findspark.init()


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

    def show_task1(self):
        df_task1 = self.spark_session.read.csv("Data/title_akas.tsv", schema=self.schema_tsv_title_akas,
                                               header=True, sep="\t")
        df_task1 = df_task1.select("title", "region").where(f.col("region") == "UA")
        df_task1.show()
        # print("I'm writing ...")
        # df_task1.write.csv("Results/task1")

    def show_task2(self):
        df_task2 = self.spark_session.read.csv("Data/name_basics.tsv", schema=self.schema_tsv_name_basics,
                                               header=True, sep="\t")
        df_task2 = df_task2.select("primaryName", "birthYear")
        df_task2 = df_task2.na.fill(0)
        df_task2 = df_task2.where((f.col("birthYear") >= 1800) & (f.col("birthYear") < 1900))
        df_task2.show()

    def show_task3(self):
        df_task3 = self.spark_session.read.csv("Data/title_basics.tsv", schema=self.schema_tsv_title_basics,
                                               header=True, sep="\t")
        df_task3 = df_task3.select("primaryTitle", "titleType", "runtimeMinutes")
        df_task3 = df_task3.na.fill(0)
        df_task3 = df_task3.where((f.col("runtimeMinutes") > 120) & (f.col("titleType") == "movie"))
        df_task3.show()
