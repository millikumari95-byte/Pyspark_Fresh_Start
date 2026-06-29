import os
import sys
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession


# ==============================================================================
# 1. ENVIRONMENT SETUP
# ==============================================================================
# Consistent Python environment for Driver (Local) and Workers (Executors)

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

#step1 read the data
data = sc.textFile("dt.txt")
print()
print("=======Rawdata=====")
data.foreach(print)
#step2 split the data with commas
mapsplit=data.map(lambda x : x.split(","))
print()
print("======mapsplit======")
mapsplit.foreach(print)
#step3 define columns using named tuple#####
from collections import namedtuple
columns = namedtuple("columns",["id","tdate","amt","category","product","mode"])
#step4 impose columns to the data splits
coldata = mapsplit.map(lambda x: columns(x[0], x[1], x[2], x[3], x[4], x[5]))
print()
print("======coldata====")
coldata.foreach(print)   # <-- use coldata instead of mapsplit

#step 5 column filter
prodfil=coldata.filter(lambda x :"Gymnastics" in x.product)
print()
print("=======prodfil=====")
prodfil.foreach(print)

####dataframe###########
# df=prodfil.toDF()
# df.show()
# df.write.parquet("file:///D:/parquetout")

csvdf = (
    spark
    .read
    .format("csv")
    .option("header", "true")   # ✅ Correct syntax
    .load("usdata.csv")
)

csvdf.show()

csvdf.show()

jsondf=(
    spark
    .read
    .format("json")
    .load("file4.json")

)
jsondf.show()

parquetdf=(
    spark
    .read
    .format("parquet")
    .load("file5.parquet")
)
print()
print("=====parquetdf======")
parquetdf.show()

orcdf=(
    spark
    .read
    .format("orc")
    .load("data.orc")
)
print("======orcdf=====")
orcdf.show()

orcdf.createOrReplaceTempView("varanasi")

spark.sql("select * from varanasi where id >0").show()
