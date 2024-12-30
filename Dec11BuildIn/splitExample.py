from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Split example ")\
        .master("local[1]")\
        .getOrCreate()


data = [
    ("James, A, Smith","2018","M",3000),
    ("Michael, Rose, Jones","2010","M",4000),
    ("Robert,K,Williams","2010","M",4000),
    ("Maria,Anne,Jones","2005","F",4000),
    ("Jen,Mary,Brown","2010","",-1)
]

column = ["firstName","dob","gender","amount"]

df = spark.createDataFrame(data,schema=column)
df.printSchema()
df.show()

from pyspark.sql.functions import col,split

df2 = df.select(split(col("firstName"),",").alias("NameArray"))
df2.printSchema()
df2.show()