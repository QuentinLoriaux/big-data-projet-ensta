from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import sqlite3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# chemins par defaut
rev_csv_path = "../../dataset/steam_reviews.csv"  
rev_parquet_path = "../../dataset/steam_reviews.parquet"
usr_sqlite_path = "../../dataset/users"
usr_parquet_path = "../../dataset/users.parquet"



def ingest_steam_reviews(csv_path=rev_csv_path, parquet_path=rev_parquet_path):

    # Besoin de specifier la memoire allouee pour eviter OutOfMemoryError
    spark = SparkSession.builder.appName("ingest").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").getOrCreate()

    # Pas besoin d'inferer le schema, il est connu
    schema = StructType([
        StructField("app_id", IntegerType(), True),
        StructField("app_name", StringType(), True),
        StructField("review_text", StringType(), True),
        StructField("review_score", IntegerType(), True),
        StructField("review_votes", IntegerType(), True)
    ])

    df = spark.read.csv(csv_path, header=True, schema=schema)
    df.write.mode("overwrite").parquet(parquet_path)

    spark.stop()




# Inutile, et crée un fichier corrompu
# On fait plutôt des opérations sur les lignes que sur les colonnes avec ce dataset
def ingest_steam_users(sqlite_path=usr_sqlite_path, parquet_path=usr_parquet_path):

    spark = SparkSession.builder.appName("ingest").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").getOrCreate()

    conn = sqlite3.connect(sqlite_path)


    # Pas assez de RAM pour charger le df en une fois
    df_chunks = pd.read_sql_query("SELECT * FROM accounts", conn, chunksize=10000)

    is_first_chunk = True
    for chunk in df_chunks:
        table = pa.Table.from_pandas(chunk)
        if is_first_chunk: # Écrire la première table dans le fichier Parquet
            pq.write_table(table, parquet_path, compression='snappy')
            is_first_chunk = False
        else:
            with open(parquet_path, 'ab') as f:
                pq.write_table(table, f, compression='snappy')
            # chunk.to_parquet(parquet_path, engine='pyarrow', compression='snappy', append=True)
    conn.close()

    # Pour chaque chunk de données
    # temp_parquet_files = []
    # for chunk in df_chunks:
    #     table = pa.Table.from_pandas(chunk)
    #     temp_parquet = tempfile.NamedTemporaryFile(delete=False, suffix='.parquet')
    #     temp_parquet_path = temp_parquet.name
    #     pq.write_table(table, temp_parquet_path, compression='snappy')
    #     temp_parquet_files.append(temp_parquet_path)
    # conn.close()

    # parquet_tables = [pq.read_table(file) for file in temp_parquet_files]
    # full_table = pa.concat_tables(parquet_tables)

    # pq.write_table(full_table, parquet_path, compression='snappy')

    # for temp_file in temp_parquet_files:
    #     os.remove(temp_file)

    spark.stop()

if __name__ == "__main__":
    from __init__ import benchmark
    # ingest_steam_reviews(rev_csv_path, rev_parquet_path)
    # ingest_steam_users(usr_sqlite_path, usr_parquet_path)

    benchmark(lambda: ingest_steam_reviews(), setSpark=False)
    