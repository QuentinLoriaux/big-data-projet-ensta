from pyspark.sql import SparkSession
import pyspark

print(pyspark.__version__)
spark = SparkSession.builder.appName("Steam_users").getOrCreate()
print(spark.version)
df = spark.read.csv("../dataset/steam_user.csv", header=True, inferSchema=True)
df.show(5)
df.printSchema()