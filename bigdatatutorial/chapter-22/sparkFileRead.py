import os
import sys


from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, collect_list, collect_set, expr

# ==============================================================================
# 1. ENVIRONMENT SETUP
# ==============================================================================
# Consistent Python environment for Driver (Local) and Workers (Executors)
valueInThisThatWeAreStoring = sys.executable
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

#conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
readTextFile = spark.sparkContext
readCSV = spark.read


readTextFile.textFile("file:///C:/Users/Milli/IdeaProjects/newpyspark/.venv/Lib/site-packages/pyspark/sql/dt.txt").foreach(print)
readCSV.csv("file:///C:/Users/Milli/IdeaProjects/newpyspark/.venv/Lib/site-packages/pyspark/sql/dt.txt").show()
