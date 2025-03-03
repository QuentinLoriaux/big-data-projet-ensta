import sqlite3
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQLiteReader").getOrCreate()

sqlite_db_path = "../dataset/users"

conn = sqlite3.connect(sqlite_db_path)
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [row[0] for row in cursor.fetchall()]
print(tables)

cursor.execute("PRAGMA table_info(accounts)")
columns = [row[1] for row in cursor.fetchall()]
cursor.execute("SELECT * FROM accounts LIMIT 100")
data = cursor.fetchall()

conn.close()

df_spark = spark.createDataFrame(data, schema=columns)

df_spark.show()

