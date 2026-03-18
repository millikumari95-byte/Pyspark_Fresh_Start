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
#Read the file (Make sure you saved the fixed k.json first!)
# #df = spark.read.format("json").load("D:\k.json")
df = spark.read.format("json").option("multiline","true").load("D:\k.json")
df.show()
df.printSchema()

flatdf=df.selectExpr("id","trainer",
                     "zeyoAddress.permanentAddress",
                     "zeyoAddress.temporaryAddress")
flatdf.show()
flatdf.printSchema()


