import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, collect_list, collect_set, expr
from pyspark.sql.functions  import *
from pyspark.sql.window import Window

"""
PySpark Complex Data Types Introduction
This script demonstrates common PySpark operations including:
1. Environment configuration for local Spark execution.
2. Basic DataFrame creation and display.
3. GroupBy and Aggregation functions (sum, count).
4. Working with Complex Data Types (collect_list, collect_set).
5. Using Spark SQL expressions (expr) for conditional logic.
6. Data deduplication and final reporting.
"""

# ==============================================================================
# 1. ENVIRONMENT SETUP
# ==============================================================================

# Force Spark to use the current virtual environment's Python executable
# This ensures consistency between the driver and workers.
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configure JAVA_HOME for Spark's JVM requirements
# Adjust these paths if your JDK location differs.
os.environ['JAVA_HOME'] = r'C:\Program Files\Amazon Corretto\jdk1.8.0_482'
os.environ['PATH'] = r'C:\Program Files\Amazon Corretto\jdk1.8.0_482\bin' + os.pathsep + os.environ['PATH']

# ==============================================================================
# 2. SPARK SESSION INITIALIZATION
# ==============================================================================
spark = SparkSession.builder \
    .appName("PySpark Complex Types Tutorial") \
    .master("local[*]") \
    .getOrCreate()
#find nth highest salary###
#find the second highest salary per department?
data = [
    ("DEPT1", 1000),
    ("DEPT1", 700),
    ("DEPT1", 500),
    ("DEPT2", 400),
    ("DEPT2", 200),
    ("DEPT3", 500),
    ("DEPT3", 200)
]

# 2. Define the column names
columns = ["department", "salary"]

# 3. Create the DataFrame
df = spark.createDataFrame(data, columns)

# 4. Show the results
df.show()

#first create window apply in dataframe
deptwindow=Window.partitionBy("department").orderBy(col("salary").desc())
denserankdf=df.withColumn("rnk",dense_rank().over(deptwindow))
denserankdf.show()
