from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("example for concat ws")\
        .master("local[1]")\
        .getOrCreate()

data =[
    ("James,,Smith",["Java","Scala","C++"],"CA"), \
    ("Michael,Rose,",["Spark","Java","C++"],"NJ"), \
    ("Robert,,Williams",["CSharp","VB"],"NV")
]

column = ["name","language","dept"]

df = spark.createDataFrame(data,schema=column)

df.show()

from pyspark.sql.functions import col,concat_ws

df2 = df.withColumn("language",concat_ws(",",col("language")))
df2.printSchema()
df2.show()