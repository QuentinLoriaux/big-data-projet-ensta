import sqlite3
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, split, explode, log1p, from_json, sum  as spark_sum
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, StringType

# On utilise toujours Parquet pour le dataset de reviews car il est meilleur en tout point que le csv

def popularity_ranking(batch_size=10_000, partition_number=10):
    """
    batch_size: Taille du batch
    partition_number: Nombre de partitions
    """

    sqlite_db_path = "../../dataset/users"

    
    columns = ["games"]
    game_schema = ArrayType(
    StructType([
        StructField("appid", IntegerType(), False),
        StructField("playtime_forever", IntegerType(), True),
        StructField("is_recommended", IntegerType(), True)
    ])
    )

    spark = SparkSession.builder.appName("recommendation")\
        .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g")\
        .getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.shuffle.spill", "true") 
    spark.sparkContext.setCheckpointDir(dirName="/tmp/spark-checkpoints")
    
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM accounts")
    total_rows = cursor.fetchone()[0]/30

    df_final = None
    offset = 0

    while offset < total_rows:
        print(f"Processing {offset}/{total_rows}")
        cursor.execute(f"SELECT games FROM accounts LIMIT {batch_size} OFFSET {offset}")
        data = cursor.fetchall()
        offset += batch_size

        if not data:
            break

        df = spark.createDataFrame(data, schema=columns)

        # Transformation de la colonne games en colonnes
        df = df.withColumn("games", from_json(col("games"), game_schema))
        df = df.withColumn("game", explode(col("games"))).select(
            col("game.appid").alias("appid"),
            col("game.playtime_forever"),
            col("game.is_recommended")
        )

        # Calcul du score de popularité pondéré
        df = df.groupBy("appid").agg(
            spark_sum("is_recommended").alias("total_recommendations"),
            spark_sum("playtime_forever").alias("total_playtime")
        )

        if df_final is None:   
            df_final = df
        else:
            df_final = df_final.union(df).groupBy("appid").agg(
                spark_sum("total_recommendations").alias("total_recommendations"),
                spark_sum("total_playtime").alias("total_playtime")
            ).repartition(partition_number).checkpoint(eager=True) # On libère la mémoire des batchs utilisés pour ne conserver que le résultat
        
        df.unpersist()
        del df
        spark.sparkContext.getOrCreate()._jvm.System.gc() 


    df_final = df_final.withColumn(
        "score", col("total_recommendations") * log1p(col("total_playtime"))
    )

    # jointure pour retrouver le nom des jeux
    df = spark.read.parquet("../../dataset/steam_reviews.parquet")
    df_final = df_final.join(df, df_final.appid == df.app_id).select(
        df.app_name, df_final.score
    ).distinct().orderBy(col("score").desc())

    conn.close()

    return df_final




def user_specific_recommendation(batch_size=500, partition_number=200):

    """
    batch_size: Taille du batch
    partition_number: Nombre de partitions
    """

    sqlite_db_path = "../../dataset/users"

    columns = ["steamID", "games", "friends"]

    schema = StructType([
        StructField("steamID", StringType(), True),
        StructField("games", StringType(), True),  # La colonne `games` contient des chaînes JSON
        StructField("friends", StringType(), True)  # La colonne `friends` contient une liste de Steam IDs
    ])

    game_schema = ArrayType(
    StructType([
        StructField("appid", IntegerType(), False),
        StructField("playtime_forever", IntegerType(), True),
        StructField("is_recommended", IntegerType(), True)
    ])
    )

    friends_schema = ArrayType(StringType())

    def process_batch(start_idx, end_idx):
    
        def get_friend_games(steamID):
            global friends_games_df
            query = f"""
                SELECT appid FROM user_games
                WHERE steamID IN (SELECT friendID FROM user_games WHERE steamID = '{steamID}')
            """
            friend_games_df = spark.sql(query)
            friend_games_df.unpersist()

            return [row["appid"] for row in friend_games_df.collect()]

        # Récupérer les utilisateurs et leurs jeux et amis
        query = f"SELECT steamID, games, friends FROM accounts LIMIT {end_idx - start_idx} OFFSET {start_idx}"
        cursor.execute(query)
        data = cursor.fetchall()

        df = spark.createDataFrame(data, schema).repartition(partition_number)

        df_exploded = df.withColumn("friends", from_json(col("friends"), friends_schema)) \
                .withColumn("games", from_json(col("games"), game_schema)) \
                .select("steamID", explode(col("friends")).alias("friendID"), explode(col("games")).alias("game")) \
                .select("steamID", "friendID", col("game.appid").alias("appid"))

        # Créer une vue temporaire faire des requêtes SQL
        df_exploded.createOrReplaceTempView("user_games")

        recommendations = []
        friends_games_df = None

        for row in df.collect():
            # Calculer la fréquence d'apparition des jeux dans les amis
            steamID = row["steamID"]
            friend_games = get_friend_games(steamID)
            if not friend_games:
                continue
            game_counts = {}
            for appid in friend_games:
                game_counts[appid] = game_counts.get(appid, 0) + 1
            
            sorted_games = sorted(game_counts.items(), key=lambda x: x[1], reverse=True)
            recommended_games = [appid for appid, _ in sorted_games[:3]]
            recommendations.append((steamID, recommended_games))
            print(f"Recommendations for {steamID}: {recommended_games}")

        # Libération de la mémoire
        df.unpersist()
        del df
        df_exploded.unpersist()
        del df_exploded
        spark.sparkContext.getOrCreate()._jvm.System.gc() 

        # Insérer ou mettre à jour les recommandations dans la nouvelle bdd
        connl = sqlite3.connect("../../dataset/recommendations.db")
        cursorl = connl.cursor()
        cursorl.execute("CREATE TABLE IF NOT EXISTS recommendations (steamID INTEGER, recommended_games TEXT)")
        
        for steamID, recommended_games in recommendations:
            cursorl.execute("INSERT INTO recommendations (steamID, recommended_games) VALUES (?, ?)",
                        (steamID, ",".join(map(str, recommended_games))))
        
        connl.commit()
        connl.close()

    spark = SparkSession.builder.appName("recommendation")\
        .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g")\
        .getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.shuffle.spill", "true") 
    spark.sparkContext.setCheckpointDir(dirName="/tmp/spark-checkpoints")
    
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(steamID) FROM accounts")
    total_users = cursor.fetchone()[0]//5000


    for i in range(0, total_users, batch_size):
        print(f"Processing {i}/{total_users}")
        process_batch(i, i + batch_size)


    conn.close()
    return None





if __name__ == "__main__":
    from __init__ import benchmark



    # benchmark(lambda: popularity_ranking(), setSpark=False)
    benchmark(lambda: user_specific_recommendation(), setSpark=False)

    # lire les recommandations
    conn = sqlite3.connect("../../dataset/recommendations.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM recommendations LIMIT 100")
    print(cursor.fetchall())
    conn.close()



