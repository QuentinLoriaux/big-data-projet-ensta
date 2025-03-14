from ingest import *
from review_ranking import *
from traitement_insultes import *
import time


def benchmark(function):
    
    start_time = time.time()
    result = function()
    end_time = time.time()

    result.show()
    print(f"Operation took {end_time - start_time:.4f} seconds")