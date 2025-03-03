from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Steam reviews") \
    .getOrCreate()

print(spark.version)

df = spark.read.csv("../dataset/users.csv", header=True, inferSchema=True)
df.show(5)
df.printSchema()

