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
rawlist=["state->TN~city->chennai", "state->kerala~city->Trivandrum"]
print()
print("=====rawlis========")
print(rawlist)
rddlis=sc.parallelize(rawlist)
print()
print("=======rddlis=====")
print(rddlis.collect())
flatrdd=rddlis.flatMap(lambda x : x.split("~"))
print(flatrdd.collect())
flatrddfilter=flatrdd.filter(lambda x : "state" in x)
print(flatrddfilter.collect())
newrdd=flatrddfilter.map(lambda x : x.replace("state->" ,""))
print(newrdd.collect())

cityrdd=flatrddfilter=flatrdd.filter(lambda x : "city" in x)
print(cityrdd.collect())
newcity=cityrdd.map(lambda x : x.replace("city->",""))
print(newcity.collect())

filerdd=sc.textFile("state.txt")
print(filerdd.collect())

usdata=sc.textFile("usdata.csv")
print(usdata.collect())
lendata=usdata.filter (lambda x : len(x)>200)
print()
print("========lendata======")
lendata.foreach(print)
flatdata=lendata.flatMap(lambda x :x.split(","))
print()
print("=====flatdata=====")
flatdata.foreach(print)
repdata=flatdata.map(lambda x : x.replace("-",""))
print()
print("=====repdata=====")
repdata.foreach(print)

adddata=repdata.map(lambda x : x + ",zeyo")
print()
print("=======adddata=====")
adddata.foreach(print)
adddata.saveAsTextFile("D:/rddout")
print("=====writtend done re --go and check  ")


