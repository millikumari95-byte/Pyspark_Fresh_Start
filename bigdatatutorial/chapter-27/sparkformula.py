import os
import sys




from pyspark.sql import SparkSession

# 1. FIX THE PYTHON WORKER CRASH:
# Explicitly tell PySpark to use your current Python executable for both driver and worker.
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# ==============================================================================
# 2. SPARK SESSION INITIALIZATION
# ==============================================================================
spark = (
    SparkSession.builder
    .appName("MySQL Integration")
    .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.3.0")  # <-- ADD THIS LINE
    .getOrCreate()
)
# sqldf = (
#     spark.read
#     .format("jdbc")
#     .option("url", "jdbc:mysql://sudhandb1.cxycqsis6mi0.ap-south-2.rds.amazonaws.com/zeyodb")
#     .option("user", "root")
#     .option("password", "Sudhan99")
#     .option("dbtable", "htest")
#     .option("driver", "com.mysql.cj.jdbc.Driver")
#     .load()
# )
#
# sqldf.show()

csvdf = (
    spark
    .read
    .format("csv")
    .option("header", "true")       # first row as column names
    .option("inferSchema", "true")  # detect column types automatically
    .load("C:/Users/Milli/IdeaProjects/SPARK/usdata.csv")
)
csvdf.show()

###SQL SHOW
csvdf.createOrReplaceTempView("rishab")
spark.sql("select * from Rishab where state='LA'").show()

##DSL SHOW
fildf=csvdf.filter("state= 'LA'")
fildf.show()

data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", "null", "cash")
]

# Using toDF() to create DataFrame
df = spark.createDataFrame(data).toDF(
    "txnno", "txndate", "amount", "category", "subcategory", "spendmode"
)

df.show()
#DSL SHOW
###1
seldf=df.select("txndate" , "amount")
seldf.show()
###2
dropdf=df.drop("txndate","amount")
dropdf.show()
###3
filterdf=df.filter("category='Exercise'")
filterdf.show()




