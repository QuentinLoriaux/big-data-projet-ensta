from pyspark.sql import SparkSession
from pyspark.sql.functions import trim
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import lit

spark = SparkSession \
    .builder \
    .appName("PySpark JDBC Query") \
    .config("spark.jars", "../../sqlite-jdbc-3.49.1.0.jar") \
    .getOrCreate()

schema = StructType([
    StructField("steamID", StringType(), True),
    StructField("games_used", BooleanType(), True),
    StructField("friends_available", BooleanType(), True),
    StructField("friends", StringType(), True),
    StructField("games", StringType(), True)
])

query = "SELECT CAST(steamID AS TEXT), CAST(friends AS TEXT) FROM accounts"
df = spark.read.jdbc(url="jdbc:sqlite:../dataset/users", table="accounts", properties={"driver": "org.sqlite.JDBC"})
df.printSchema()
# attempt to purify the data
df = df.withColumn("friends", lit("toto"))
df = df.na.fill({"friends": "tqta"})
df.drop("games")
df.show(2)



# df = spark.read.jdbc(url="jdbc:sqlite:../dataset/users", table="accounts", properties={"driver": "org.sqlite.JDBC"})
df = spark.createDataFrame(df.rdd, schema)
df.printSchema()
# attempt to cast the columns to string
df = df.withColumn("steamID", df["steamID"].cast("string"))
df = df.withColumn("friends", col("friends").cast("string"))
# df = df.withColumn("steamID", trim(df["steamID"]))
# df = df.withColumn("steamID", df["steamID"].cast("string"))
# df = df.drop("friends")
df = df.drop("games")
df.printSchema()
df.show(2)

# the issue is likely that the data is not deserialized correctly by the jdbc reader
# to fix this, we might have to convert it to strings before reading it, through sqlite3 for instance, while parallelizing the data