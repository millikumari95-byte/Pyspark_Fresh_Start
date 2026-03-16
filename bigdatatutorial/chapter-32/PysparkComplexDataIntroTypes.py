
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, collect_list, collect_set, expr

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

# ==============================================================================
# 3. BASIC AGGREGATIONS & COMPLEX TYPES (Part 1)
# ==============================================================================

# Sample transactional data
data = [
    ("sai", "chn", 40),
    ("zeyo", "hyd", 10),
    ("sai", "hyd", 20),
    ("zeyo", "chn", 20),
    ("sai", "chn", 10),
    ("zeyo", "hyd", 10)
]

# Create initial DataFrame
df = spark.createDataFrame(data, ["name", "city", "amount"])
print("--- Raw Transactional Data ---")
df.show()

# Simple GroupBy: Total amount per name
print("--- Aggregation: Sum by Name ---")
aggdf = df.groupBy("name").agg(sum("amount").alias("total"))
aggdf.show()

# Adding Count: Number of transactions per name
print("--- Aggregation: Sum and Count ---")
aggdf = df.groupBy("name").agg(
    sum("amount").alias("total"),
    count("amount").alias("cnt")
)
aggdf.show()

# Introducing collect_list: Gathers all values into an array (allows duplicates)
print("--- Aggregation: collect_list (Includes Duplicates) ---")
aggdf = df.groupBy("name").agg(
    sum("amount").alias("total"),
    count("amount").alias("cnt"),
    collect_list("amount").alias("amt_collect")
)
aggdf.show()

# Introducing collect_set: Gathers unique values into an array
print("--- Aggregation: collect_set (Unique Values Only) ---")
aggdf = df.groupBy("name").agg(
    sum("amount").alias("total"),
    count("amount").alias("cnt"),
    collect_list("amount").alias("amt_collect"),
    collect_set("amount").alias("amt_set")
)
aggdf.show()

# Multi-column GroupBy: Aggregating by Name AND City
print("--- Aggregation: GroupBy Name and City ---")
aggdf = df.groupBy("name", "city").agg(
    sum("amount").alias("total"),
    count("amount").alias("cnt"),
    collect_list("amount").alias("amt_collect"),
    collect_set("amount").alias("amt_set")
)
aggdf.show()

# ==============================================================================
# 4. CONDITIONAL LOGIC WITH expr() AND CASE-WHEN
# ==============================================================================

# Customer demographic data
customer_data = [
    (1, "Alice", 25, "F"),
    (2, "Bob", 40, "M"),
    (3, "Raj", 46, "M"),
    (4, "Sekar", 66, "M"),
    (5, "Jhon", 47, "M"),
    (6, "Timoty", 28, "M"),
    (7, "Brad", 90, "M"),
    (8, "Rita", 34, "F")
]

columns = ["customer_id", "name", "age", "gender"]
df_customers = spark.createDataFrame(customer_data, schema=columns)
print("--- Customer Data ---")
df_customers.show()

# Define age groups using SQL-style CASE-WHEN logic inside expr()
print("--- Applying Age Groups via CASE-WHEN ---")
df_with_groups = df_customers.withColumn("age_group", expr("""
    CASE 
        WHEN age >= 19 AND age <= 35 THEN '19-35'
        WHEN age >= 36 AND age <= 50 THEN '36-50'
        WHEN age > 51 THEN '51+'
        ELSE 'NA'
    END
"""))
df_with_groups.show()

# Aggregate to count customers in each age group
print("--- Count by Age Group ---")
df_age_summary = df_with_groups.groupby("age_group").agg(
    count("age_group").alias("count")
)
df_age_summary.show()

# ==============================================================================
# 5. PRODUCT SALES ANALYSIS & DEDUPLICATION
# ==============================================================================

# Sales data with potential duplicate transactions
sales_data = [
    ("2020-05-30", "Headphone"),
    ("2020-06-01", "Pencil"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "Basketball"),
    ("2020-06-01", "Book"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "T-Shirt")
]

sales_schema = ["sell_date", "product"]
df_sales = spark.createDataFrame(sales_data, sales_schema)
print("--- Raw Sales Data ---")
df_sales.show()

# Aggregation without deduplication: Shows all products sold per day
print("--- Daily Product Collection (Before Deduplication) ---")
agg_sales_raw = df_sales.groupby("sell_date").agg(
    collect_set("product").alias("products"),
    count("product").alias("null_sell")
)
agg_sales_raw.show()

# Aggregation with deduplication: Removes duplicate (Date, Product) pairs first
print("--- Daily Product Collection (After dropDuplicates) ---")
agg_sales_clean = df_sales.dropDuplicates().groupby("sell_date").agg(
    collect_set("product").alias("products"),
    count("product").alias("null_sell")
)
agg_sales_clean.show()
