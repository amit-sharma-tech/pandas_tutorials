from pyspark.sql import SparkSession
from pyspark.sql.functions import col,expr
spark = SparkSession.builder\
        .appName("expre example")\
        .master("local[1]")\
        .getOrCreate()


# data = [
#     ("James","M",60000),("Michael","M",70000),
#     ("Robert",None,400000),("Maria","F",500000),
#     ("Jen","",None)
# ]

# df = spark.createDataFrame(data,schema=["name","gender","amount"])

# df.show()

# df2= df.withColumn("gender",expr(
#     "CASE WHEN gender ='M' THEN 'Male'"+
#     "WHEN gender ='F' THEN 'Female'"+
#     "ELSE 'unknown' END"
# ))

# df2.show()

##example of existing column value for expression

data = [("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)]
df3 = spark.createDataFrame(data).toDF("date","increment")

df3.select(df3.date,df3.increment,expr("add_months(date,increment)").alias("inc_date")).show()

# df3.select(df3.date,df3.increment,expr("add_months(date,1)").alias("inc_date")).show()