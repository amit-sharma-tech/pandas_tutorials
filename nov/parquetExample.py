from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType

spark = SparkSession.builder\
        .appName("read csv file to write in parquet")\
        .master("local[1]")\
        .getOrCreate()

readCsv = spark.read.format("csv")\
        .option("header",True)\
        .option("inferSchema",True)\
        .load("../dataSource/2010-summary.csv")

readCsv.printSchema()


writeParquet = readCsv.write.mode("overwrite").parquet("../outputResult/summary.parquet")

readparquet = spark.read.parquet("../outputResult/summary.parquet")
# readparquet.printSchema()
# readparquet.show(10)

#read in sql format

sparkSql = readparquet.createOrReplaceTempView("summary")

sqlView = spark.sql("select * from summary limit 20").show() 

#Repartition query

# readCsv.write.partitionBy("DEST_COUNTRY_NAME").mode("overwrite").parquet("../outputResult/summary.parquet")

# readSpe = spark.read.parquet("../outputResult/summary.parquet/DEST_COUNTRY_NAME=India")
# readSpe.show()

#read global view

# spark.sql("create temporary view countryName using parquet options(path\"../outputResult/summary.parquet/DEST_COUNTRY_NAME=India\")")
# spark.sql("select * from countryName").show()