from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Steam reviews").getOrCreate()

df = spark.read.csv("../dataset/users.csv", header=True, inferSchema=True)
df.printSchema()
df.show(5)

