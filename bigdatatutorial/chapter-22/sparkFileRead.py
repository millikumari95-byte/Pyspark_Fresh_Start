import os
import sys


from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession


# ==============================================================================
# 1. ENVIRONMENT SETUP
# ==============================================================================
# Consistent Python environment for Driver (Local) and Workers (Executors)

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable



# ==============================================================================
# 2. SPARK SESSION INITIALIZATION
# ==============================================================================
# .master("local[*]") uses all available cores on your machine.
spark = SparkSession.builder \
    .appName("Milli") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.default.parallelism", "2") \
    .getOrCreate()



# ==============================================================================
# 3. File Read from csv and txt
# ==============================================================================
readTextFile = spark.sparkContext
readCSV = spark.read


readTextFile.textFile("file:///C:/Users/Milli/IdeaProjects/newpyspark/.venv/Lib/site-packages/pyspark/sql/dt.txt").foreach(print)
readCSV.csv("file:///C:/Users/Milli/IdeaProjects/newpyspark/.venv/Lib/site-packages/pyspark/sql/dt.txt").show()
