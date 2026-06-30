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
spark = SparkSession.builder \
    .appName("Milli") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.default.parallelism", "2") \
    .getOrCreate()


# ==============================================================================
# 3. RUNNING OPERATIONS
# ==============================================================================
data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()


data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
df1.show()






data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]



cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()

df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")

spark.sql("select id ,tdate from df").show()
spark.sql("select * from df where category ='Exercise'").show()
spark.sql("select id,tdate,category,spendby from df where category ='Exercise' and spendby='cash' ").show()
spark.sql("select id,tdate,category,spendby from df where category in ('Exercise' ,'Gymnastics')").show()

spark.sql("select count(*) from df ").show()

##conditional statement-------

spark.sql("select *,case when spendby='cash' then 1 else 0 end from df ").show()
spark.sql("select *,case when spendby='cash'  then 1 else 0 end as status from df ").show()

#concat two columns
spark.sql("select id,category,concat(id,'-',category ) as condata from df ").show()
spark.sql("select id,category,product,concat_ws('-',id,category,product ) as condata from df ").show()

#Lower case
spark.sql("select category,lower(category) from df").show()
#upper case data
spark.sql("select category,upper(category) from df").show()

##CEIL
spark.sql("select  amount, ceil(amount) as ceil from df ").show()

#ROUND
spark.sql("select  amount, round(amount) as round from df ").show()

##Replace Null
spark.sql("select  product,coalesce (product,'NA') as nullrep from df ").show()
#Remove empty space
spark.sql("select  trim(product) from df ").show()

#distinct columns values
spark.sql("select distinct category from df ").show()

#substring
spark.sql("select substring (product,1,10)  as sub  from df ").show()

##substring split operations
spark.sql("select product , split (product,' ')[0] as split from df ").show()

#Union All
spark.sql("select * from df union all select * from df1 ").show()
#UNION
spark.sql("select * from df union select * from df1 ").show()
#GROUP BY---single column
spark.sql("select category ,sum(amount ) as sum from df group by category  ").show()
#group by two columns
spark.sql("select category,spendby ,sum(amount ) as total from df group by category,spendby  ").show()
#count also

spark.sql("select category,spendby ,sum(amount ) as sum,count(amount) as cnt  from df group by category,spendby  ").show()
#Max
spark.sql ("select category ,max(amount) as max from df group by category ").show()
#order by
spark.sql ("select category ,max(amount) as max from df group by category order  by category ").show()
#window Row number
spark.sql("select category,amount,row_number() OVER (partition by category order by amount desc) AS row_number From df ").show()
#Window Rank_number
spark.sql("select category,amount,dense_rank() OVER (partition by category order by amount desc) AS dense_rank From df ").show()
#RANK
spark.sql("select category,amount,rank() OVER (partition by category order by amount desc) AS rank From df ").show()
#WINDOW LEAD OPERATION
spark.sql("select category,amount,lead(amount) OVER (partition by category order by amount desc) AS lead From df ").show()
#WINDOW LAG
spark.sql("select category,amount,lag(amount) OVER (partition by category order by amount desc) AS lag From df ").show()
#Having functions
spark.sql("select category ,count(category) as cnt from df group by category having count(category)>1").show()
#join
#inner join
spark.sql("select a.* ,b.product from cust a join prod b on a.id=b.id").show()
#left join
spark.sql("select a.* ,b.product from cust a left join prod b on a.id=b.id").show()
#Right join
spark.sql("select a.* ,b.product from cust a right join prod b on a.id=b.id").show()
#FULL JOIN
spark.sql("select a.* ,b.product from cust a full join prod b on a.id=b.id").show()
#Left anti join
spark.sql("select a.*  from cust a left anti  join prod b on a.id=b.id").show()