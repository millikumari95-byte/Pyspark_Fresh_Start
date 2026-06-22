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
    .config("spark.default.parallelism", "1") \
    .getOrCreate()

sc= spark.sparkContext


ls=[1,2,3]
print(ls)

rddls=sc.parallelize(ls)
print("====rddls===")
print(rddls.collect())


addrdd=rddls.map(lambda x:x + 2)
print("======addrdd===")
print(addrdd.collect())

lis=["A~B","C~D"]
print(lis)
rdds=sc.parallelize(lis)
print(rdds.collect())

mapsplit=rdds.map(lambda x : x.split("~"))
print(mapsplit.collect())
flatrdd=rdds.flatMap(lambda x : x.split("~"))
print(flatrdd.collect())




