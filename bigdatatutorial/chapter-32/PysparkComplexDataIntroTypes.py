
"""
PySpark Complex Data Types & Aggregations Tutorial
==================================================

THEORY: Understanding PySpark Aggregations and Complex Types
-----------------------------------------------------------
1. Aggregations (groupBy):
   Aggregations are 'Wide Transformations' because they require a 'Shuffle' operation.
   Data with the same key is moved across the cluster to the same executor so it 
   can be processed together (e.g., to calculate a sum or count).

2. Complex Data Types (Arrays):
   - collect_list: An aggregation function that returns an array of all values in 
     the group. It preserves duplicates and maintains the order (non-deterministically).
   - collect_set: Similar to collect_list, but returns an array of UNIQUE values. 
     This is useful for deduplicating items within a group.

3. The expr() Function:
   Bridges the gap between the DataFrame API and Spark SQL. It allows you to write 
   SQL-like expressions (like CASE-WHEN or complex math) directly within 
   DataFrame transformations like withColumn() or select().
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, collect_list, collect_set, expr

# ==============================================================================
# 1. ENVIRONMENT SETUP
# ==============================================================================
# Consistent Python environment for Driver (Local) and Workers (Executors)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Ensure JVM can find the Java Runtime
os.environ['JAVA_HOME'] = r'C:\Program Files\Amazon Corretto\jdk1.8.0_482'
os.environ['PATH'] = r'C:\Program Files\Amazon Corretto\jdk1.8.0_482\bin' + os.pathsep + os.environ['PATH']

# ==============================================================================
# 2. SPARK SESSION INITIALIZATION
# ==============================================================================
# .master("local[*]") uses all available cores on your machine.
spark = SparkSession.builder \
    .appName("PySpark Complex Types Tutorial") \
    .master("local[*]") \
    .getOrCreate()

# ==============================================================================
# 3. BASIC AGGREGATIONS & COMPLEX TYPES (ARRAY TYPE)
# ==============================================================================

# Sample transactional data representing (Name, City, TransactionAmount)
transaction_data = [
    ("sai", "chn", 40),
    ("zeyo", "hyd", 10),
    ("sai", "hyd", 20),
    ("zeyo", "chn", 20),
    ("sai", "chn", 10),
    ("zeyo", "hyd", 10)
]

# Create DataFrame with explicit column names
df_transactions = spark.createDataFrame(transaction_data, ["name", "city", "amount"])
print("--- Raw Transactional Data ---")
df_transactions.show()

# 3.1 Basic Aggregation: Summing values by category
print("--- Aggregation: Sum by Name ---")
# alias() is crucial for readability in final reports
agg_sum_df = df_transactions.groupBy("name").agg(sum("amount").alias("total_amount"))
agg_sum_df.show()

# 3.2 Complex Types: collect_list (Array of all values including duplicates)
print("--- Aggregation: collect_list (Capturing All Values) ---")
# This transforms standard columns into an ArrayType column
agg_complex_df = df_transactions.groupBy("name").agg(
    sum("amount").alias("total"),
    count("amount").alias("transaction_count"),
    collect_list("amount").alias("amount_history")
)
agg_complex_df.show()

# 3.3 Complex Types: collect_set (Array of unique values)
print("--- Aggregation: collect_set (Unique Values Only) ---")
# Useful for finding unique cities or distinct price points per user
agg_unique_df = df_transactions.groupBy("name").agg(
    sum("amount").alias("total"),
    collect_list("amount").alias("all_amounts"),
    collect_set("amount").alias("unique_amounts")
)
agg_unique_df.show()

# ==============================================================================
# 4. CONDITIONAL LOGIC & DATA CATEGORIZATION (expr)
# ==============================================================================

# Customer demographic data (ID, Name, Age, Gender)
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

df_customers = spark.createDataFrame(customer_data, ["customer_id", "name", "age", "gender"])

print("--- Applying Age Segments via SQL expressions ---")
# Using expr() allows us to write standard SQL CASE statements
# This is often more readable than nested when() functions for complex logic.
df_segmented = df_customers.withColumn("age_segment", expr("""
    CASE 
        WHEN age BETWEEN 19 AND 35 THEN 'Young Professional'
        WHEN age BETWEEN 36 AND 50 THEN 'Mid-Career'
        WHEN age > 50 THEN 'Senior'
        ELSE 'Other'
    END
"""))
df_segmented.show()

# ==============================================================================
# 5. DATA DEDUPLICATION & REPORTING
# ==============================================================================

# Sales log with potential duplicate entries
sales_log = [
    ("2020-05-30", "Headphone"),
    ("2020-06-01", "Pencil"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "Basketball"),
    ("2020-06-01", "Book"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "T-Shirt")
]

df_sales = spark.createDataFrame(sales_log, ["sell_date", "product"])

print("--- Deduplicated Daily Sales Report ---")
# dropDuplicates() ensures we don't count the same product twice for the same day
# collect_set() then aggregates these unique items into an array
df_daily_report = df_sales.dropDuplicates().groupBy("sell_date").agg(
    collect_set("product").alias("unique_products_sold"),
    count("product").alias("total_unique_items")
)
df_daily_report.orderBy("sell_date").show()
