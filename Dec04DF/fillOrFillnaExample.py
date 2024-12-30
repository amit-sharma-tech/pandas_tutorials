from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("fill or fillna example")\
        .master("local[1]")\
        .getOrCreate()

filePath = "../dataSource/zipcodeNew.csv"
df = spark.read.options(header='true',inferSchema='true')\
    .csv(filePath)

df.printSchema()
df.show()
df.na.fill(value=0).show()

df.na.fill(value=0,subset=["population"]).show()

df.na.fill("").show()

df.na.fill({"city": "unknown", "type": ""}) \
    .show()