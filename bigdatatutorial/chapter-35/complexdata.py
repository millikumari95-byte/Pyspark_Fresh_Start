import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import col, struct, count, when,expr,explode

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

# #PRACTICSE 1
# data = """
# {
#     "id" : 1,
#     "trainer" : "sai",
#     "zeyoAddress" : {
#        "user":{
#             "permanentAddress" : "Hyderabad",
#             "temporaryAddress" : "Chennai"
#             }
#         }
# }
# """
#
# df = spark.read.json(sc.parallelize([data]))
# df.show()
# df.printSchema()
# flatdf=df.selectExpr("id","trainer",
#                      "zeyoAddress.user.permanentAddress",
#                      "zeyoAddress.user.temporaryAddress")
# flatdf.show()
# flatdf.printSchema()
#
# #Practicse 2
# data="""
#
#  {
#     "place": "Hyderabad",
#     "user": {
#         "name": "zeyo",
#         "address": {
#             "number": "40",
#             "street": "ashok nagar",
#             "pin": "400209"
#         }
#     }
# }
# """
# df = spark.read.json(sc.parallelize([data]))
# df.show()
# df.printSchema()
# flatdf=df.selectExpr(
#             "place",
#                       "user.address.number",
#                       "user.address.pin",
#                       "user.address.street",
#                        "user.name"
#
#
# )
# flatdf.show()
# flatdf.printSchema()
# #PRACTICSE 3
# data='''
# {
#   "first_name":"Rajeev",
#   "last_name": "Sharma",
#   "email_address": "rajeev@ezeelive.com",
#   "is_alive": true,
#   "age": 30,
#   "height_cm": 185.2,
#   "billing_address": {
#     "address": "502, Main Market, Evershine City, Evershine, Vasai East",
#     "city": "Vasai Raod, Palghar",
#     "state": "Maharashtra",
#     "postal_code": "401208"
#   },
#   "shipping_address": {
#     "address": "Ezeelive Technologies, A-4, Stattion Road, Oripada, Dahisar East",
#     "city": "Mumbai",
#     "state": "Maharashtra",
#     "postal_code": "400058"
#   },
#   "date_of_birth": null
# }
# '''
# df=spark.read.json(sc.parallelize([data]))
# df.show()
# df.printSchema()
#
# flatdf=df.selectExpr(
#             "age",
#                       "billing_address.address as billing_address",
#                       "billing_address.city as billing_city",
#                       "billing_address.postal_code as billing_postal_code",
#                       "billing_address.state as billing_state",
#                   "date_of_birth",
#                   "email_address",
#                   "first_name",
#                    "height_cm",
#                   "is_alive",
#                   "last_name",
#                      "shipping_address.address as shipping_address",
#                      "shipping_address.city as shipping_city",
#                      "shipping_address.postal_code as shipping_postal_code",
#                      "shipping_address.state as shipping_state"
#
#
#
# )
# flatdf.show()
# flatdf.printSchema()
#
# #PRACTICSE 5
#data = """
# {
#     "id" : 1,
#     "trainer" : "sai",
#     "zeyoAddress" : {
#
#             "permanentAddress" : "Hyderabad",
#             "temporaryAddress" : "Chennai"
#
#         }
# }
# """
# df = spark.read.json(sc.parallelize([data]))
# df.show()
# df.printSchema()
#
# withdf = (
#     df.withColumn("permanentAddress", col("zeyoAddress.permanentAddress"))
#     .withColumn("temporaryAddress", col("zeyoAddress.temporaryAddress"))
#     .drop("zeyoAddress")
# )
# withdf.show()
# withdf.printSchema()
# # #Practicse 6
# data='''
# {
#   "first_name":"Rajeev",
#   "last_name": "Sharma",
#   "email_address": "rajeev@ezeelive.com",
#   "is_alive": true,
#   "age": 30,
#   "height_cm": 185.2,
#   "billing_address": {
#     "address": "502, Main Market, Evershine City, Evershine, Vasai East",
#     "city": "Vasai Raod, Palghar",
#     "state": "Maharashtra",
#     "postal_code": "401208"
#   },
#   "shipping_address": {
#     "address": "Ezeelive Technologies, A-4, Stattion Road, Oripada, Dahisar East",
#     "city": "Mumbai",
#     "state": "Maharashtra",
#     "postal_code": "400058"
#   },
#   "date_of_birth": null
# }
# '''
# df=spark.read.json(sc.parallelize([data]))
# df.show()
# df.printSchema()
#
# withdf=(
#             df.withColumn("billingAddress",col("billing_address.address"))
#            .withColumn("billing_city",col("billing_address.city"))
#            .withColumn("billing_postal_code",col("billing_address.postal_code"))
#             .withColumn("billing_state",col("billing_address.state"))
#             .drop("billing_address")
#
#             .withColumn("shippingAddress",col("shipping_address.address"))
#             .withColumn("shipping_city",col("shipping_address.city"))
#             .withColumn("shipping_postal_code",col("shipping_address.postal_code"))
#             .withColumn("shipping_state",col("shipping_address.state"))
#             .drop("shipping_address"))
#
#
#
#
#
# withdf.show()
# withdf.printSchema()
#Prcticse 7
data = """
{
    "id" : 1,
    "trainer" : "sai",
    
"zeyoStudents" :[
            "Archana",
            "Rishi"
            ]

        
}
"""
df=spark.read.json(sc.parallelize([data]))
df.show()
df.printSchema()

flatdf=df.selectExpr("id",
                     "trainer",
                     "explode(zeyoStudents) as zeyoStudents"

)

flatdf.show()
flatdf.printSchema()

withdf = df.withColumn("zeyoStudents", explode(col("zeyoStudents")))
withdf.show()
withdf.printSchema()

