import os
import sys
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, collect_list, collect_set, expr

# ==============================================================================
# 1. ENVIRONMENT SETUP
# ==============================================================================
# Consistent Python environment for Driver (Local) and Workers (Executors)
#valueInThisThatWeAreStoring = sys.executable
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


print("====started====")
a=2
print(a)

b=a+2
print(b)

c="zeyobron"
print(c)

d=c+"analytics"
print(d)

ls=[1,2,3]
print(ls)

rddls=sc.parallelize(ls)
print("====rddls===")
print(rddls.collect())


addrdd=rddls.map(lambda x:x + 2)
print("======addrdd===")
print(addrdd.collect())


