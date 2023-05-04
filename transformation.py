import pyspark
from pyspark.sql import SparkSession

spark_session = SparkSession.builder.master("local").appName("PySparkProject").getOrCreate()

def getDataFromFile(pathToFile, schema):
    data = spark_session.read.format("csv").option("compression", "gzip").option("header", True).option("sep", "\t").schema(schema).load(pathToFile)
    # data.printSchema()
    return data


