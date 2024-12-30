from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("lit example")\
        .master("local[1]")\
        .getOrCreate()


data = [
    ("James","M",60000),("Michael","M",70000),
    ("Robert",None,400000),("Maria","F",500000),
    ("Jen","",None)
]
column = ["name","gender","amount"]

df = spark.createDataFrame(data,schema=column)

from pyspark.sql.functions import col,lit

df2 = df.select(col("name"),col("gender"),col("amount"),lit("NEW").alias("lit_value"))
df2.show()