from pyspark.sql import SparkSession
from pyspark.sql.functions import col,substring

spark = SparkSession.builder\
        .appName("Sub String example")\
        .master("local[1]")\
        .getOrCreate()


data = [(1,"20200828"),(2,"20180525")]

column = ["id","date"]

df = spark.createDataFrame(data,schema=column)

df2= df.withColumn("year",substring('date',1,4))\
    .withColumn("month",substring("date",5,2))\
    .withColumn("day",substring('date',7,2))

df2.printSchema()
df2.show()
