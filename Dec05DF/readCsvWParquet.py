from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Read csv and write in parquet file")\
        .master("local[1]")\
        .getOrCreate()


df  =    spark.read.format("csv")\
        .option("header",True)\
        .option("inferSchema",True)\
        .load("../dataSource/2010-summary.csv")

df.printSchema()
df.show()


###write this file into parquet

df.write.mode("overwrite")\
        .option("header",True)\
        .parquet("../outputResult/parquet05")

##SQL query dataFrame

parqDF = df.createOrReplaceTempView("zipcode")
parqSQL = spark.sql("select * from zipcode where DEST_COUNTRY_NAME='Malta'").show()


###crate temp view on parquet file

spark.sql("create temporary view person using parquet options(path \"../outputResult/parquet05\")")
spark.sql("select * from person ").show()

# df.write.partitionBy("DEST_COUNTRY_NAME").mode("overwrite").parquet("../outputResult/parquet05")