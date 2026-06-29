import os
import sys




from pyspark.sql import SparkSession

# ==============================================================================
# 2. SPARK SESSION INITIALIZATION
# ==============================================================================
spark = SparkSession.builder \
    .appName("Milli") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.default.parallelism", "2") \
    .getOrCreate()


# ==============================================================================
# 3. RUNNING OPERATIONS
# ==============================================================================

df = spark.read.option("header", "true").csv("C:/data/df.csv")
df.createOrReplaceTempView("df")

spark.sql("SELECT * FROM df LIMIT 5").show()