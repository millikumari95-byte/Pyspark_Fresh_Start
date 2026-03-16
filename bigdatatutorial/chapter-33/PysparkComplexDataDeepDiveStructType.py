"""
PySpark Deep Dive: Window Functions & StructType
===============================================

THEORY: Window Functions
------------------------
Window functions allow you to perform calculations across a set of rows that are 
related to the current row. Unlike 'groupBy' aggregations, window functions 
DO NOT collapse rows; each row retains its identity while gaining an 
aggregated/ranked value.

1. partitionBy: Defines the groups (windows) the data is split into.
2. orderBy: Determines the sort order within each window.
3. Ranking Functions:
   - row_number(): Unique sequence (1, 2, 3...) regardless of ties.
   - rank(): Skips numbers after ties (1, 2, 2, 4...).
   - dense_rank(): No gaps after ties (1, 2, 2, 3...).

THEORY: StructType & StructField
-------------------------------
These classes are used to define the schema of a DataFrame programmatically. 
A 'StructType' is a collection of 'StructFields', which define the column name, 
data type, and nullability. This is essential for:
- Defining schemas for semi-structured data (JSON/Parquet).
- Creating nested columns (columns within columns).
"""

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

# ==============================================================================
# 2. WINDOW FUNCTIONS: FINDING Nth HIGHEST SALARY
# ==============================================================================

# Sample Employee Salary Data
salary_data = [
    ("DEPT1", 1000),
    ("DEPT1", 700),
    ("DEPT1", 500),
    ("DEPT2", 400),
    ("DEPT2", 200),
    ("DEPT3", 500),
    ("DEPT3", 200),
    ("DEPT3", 500)  # Duplicate for dense_rank demo
]

# Defining schema implicitly for this simple case
df_salaries = spark.createDataFrame(salary_data, ["department", "salary"])

print("--- Calculating Dense Rank within Departments ---")
# Step A: Define the Window Spec
# We group by department and sort salaries in descending order
dept_window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

# Step B: Apply the window function
# dense_rank() ensures that tied salaries get the same rank, and no ranks are skipped
df_ranked = df_salaries.withColumn("rank", dense_rank().over(dept_window_spec))
df_ranked.show()

# Step C: Filter for the 2nd highest salary (n=2)
print("--- 2nd Highest Salary per Department ---")
df_second_highest = df_ranked.filter("rank = 2").drop("rank")
df_second_highest.show()

