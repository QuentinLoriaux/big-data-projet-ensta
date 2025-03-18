from ingest import *
from review_ranking import *
from insult_ranking import *
import time


def benchmark(function, show=True, setSpark=True):
    
    if setSpark:
        spark = SparkSession.builder.appName("benchmark").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").getOrCreate()

    start_time = time.time()
    result = function()
    end_time = time.time()

    if show and result:
        result.show()

    if setSpark:
        spark.catalog.clearCache()
        spark.stop()

    print(f"Operation took {end_time - start_time:.4f} seconds")