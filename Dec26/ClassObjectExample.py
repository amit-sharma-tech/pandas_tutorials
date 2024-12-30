from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder\
        .appName("Class object example")\
        .master("local[1]")\
        .getOrCreate()

data = [("James",23),("Ann",40)]

df = spark.createDataFrame(data).toDF("name.fname","gender")

df.printSchema()
df.show()

#Using DataFrame Object