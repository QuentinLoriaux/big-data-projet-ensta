from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# Initialize Spark Session
spark = SparkSession.builder.appName("SwearWordCount").getOrCreate()
sc = spark.sparkContext  # Get SparkContext

# Start timer
start_time = time.time()

# Load CSV file as DataFrame
df = spark.read.csv("../dataset/steam_reviews.csv", header=True, inferSchema=True)

# Load swear words into a list and broadcast it
with open("../dataset/swearWords.txt", "r") as f:
    swear_list = [line.strip().lower() for line in f.readlines()]
swear_broadcast = sc.broadcast(set(swear_list))

# Convert DataFrame to RDD
rdd = df.select("review_text").rdd.flatMap(lambda x: x)

# Tokenize the reviews and filter swear words
rdd_tokens = rdd.flatMap(lambda text: text.lower().split())  # Tokenization
rdd_swears = rdd_tokens.filter(lambda word: word in swear_broadcast.value)  # Filter swear words

# Count occurrences of swear words
rdd_word_counts = rdd_swears.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Convert RDD back to DataFrame for display
swear_df = spark.createDataFrame(rdd_word_counts, ["word", "count"]).orderBy("count", ascending=False)

# Show results
swear_df.show()

# End timer
end_time = time.time()
print(f"Operation took {end_time - start_time:.4f} seconds")
