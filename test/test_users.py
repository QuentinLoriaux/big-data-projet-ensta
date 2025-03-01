from pyspark.sql import SparkSession
import pyspark

print(pyspark.__version__)
spark = SparkSession.builder.appName("Steam_users").getOrCreate()
print(spark.version)
df = spark.read.json("../dataset/users")
df.show(5)
df.printSchema()