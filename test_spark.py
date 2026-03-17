import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = r'C:\Program Files\Amazon Corretto\jdk1.8.0_482'
os.environ['PATH'] = r'C:\Program Files\Amazon Corretto\jdk1.8.0_482\bin' + os.pathsep + os.environ['PATH']

print("Starting Spark Session...")
try:
    spark = SparkSession.builder \
        .appName("Test Spark") \
        .master("local[*]") \
        .getOrCreate()

    print("Spark Session Created.")
    df = spark.createDataFrame([("Alice", 1)], ["name", "id"])
    df.show()
    print("Spark Show Finished.")
except Exception as e:
    print(f"Error: {e}")
finally:
    if 'spark' in locals():
        spark.stop()
