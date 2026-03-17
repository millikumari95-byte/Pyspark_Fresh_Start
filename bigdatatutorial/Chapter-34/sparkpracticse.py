import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, dense_rank
from pyspark.sql.window import Window

# ==============================================================================
# 1. ENVIRONMENT SETUP
# ==============================================================================
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = r'C:\Program Files\Amazon Corretto\jdk1.8.0_482'
os.environ['PATH'] = r'C:\Program Files\Amazon Corretto\jdk1.8.0_482\bin' + os.pathsep + os.environ['PATH']

spark = SparkSession.builder \
    .appName("PySpark Window and Struct Tutorial") \
    .master("local[*]") \
    .getOrCreate()
sc=spark.sparkContext
lis=[1,4,6,7]
print(lis)
rddlis=sc.parallelize(lis)
print(rddlis)
addlis=rddlis.map(lambda x : x + 2)
print(addlis.collect())
