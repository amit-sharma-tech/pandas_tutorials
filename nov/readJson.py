from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,BooleanType,DoubleType

spark = SparkSession.builder\
        .appName("read json file")\
        .master("local[1]")\
        .getOrCreate()

readJson = spark.read.json("../dataSource/zipcodes.json")

readJson.printSchema()
readJson.show(2)

#read multiline json file

multiLine_df = spark.read.option("multiline",True)\
                .json("../dataSource/zipcodes.json")

multiLine_df.show(2)


spark.sql("create or replace temporary view zipcode using json options " + "(path '../dataSource/zipcodes.json')")

spark.sql("select * from zipcode").show(2)

multiLine_df.write.mode("overwrite").json("../outputResult/zipcodes.json")

#write in parquet
multiLine_df.write.mode("overwrite").parquet("../outputResult/zipcode.parquet")

print("done...")