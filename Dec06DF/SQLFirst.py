from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("SQL example")\
        .master("local[1]")\
        .getOrCreate()
 

data = [("Anna",1000),("")]


