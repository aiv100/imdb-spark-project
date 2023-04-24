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

    def show_task1(self):
        df_task1 = self.spark_session.read.csv("Data/title_akas.tsv", schema=self.schema_tsv_title_akas,
                                               header=True, sep="\t")
        # df_task1.show(truncate=False)
        df_result = df_task1.select("title", "region").where(f.col("region") == "UA")
        df_result.show()
        print("I'm writing ...")
        df_result.write.csv("Results/task1")
        # df_result.write.format('csv').option('header','true').option('sep','\t').save('Results/titanic.tsv')
