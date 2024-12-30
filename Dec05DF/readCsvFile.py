from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Read csv file")\
        .master("local[1]")\
        .getOrCreate()

# df = spark.read.csv("../dataSource/2010-summary.csv")

##Another way

# df =    spark.read.format("csv")\
#         .option("header",True)\
#         .option("inferSchema",True)\
#         .load("../dataSource/2010-summary.csv")

### Another ways

# df = spark.read.format("csv")\
#      .options(header=True,inferSchema=True)\
#      .load("../dataSource/2010-summary.csv")

###Another ways

options = {
    "header":"True",
    "delimiter":",",
    "inferSchema":"True"
}

df = spark.read.format("csv")\
    .options(**options).csv("../dataSource/2010-summary.csv")

df.printSchema()
df.show()

###iting Frame to csv file

# df.write.option("header",True)\
#     .csv("../outputResult/zipcodes05")

###Another way to write

df.write.mode("overwrite")\
    .option("header",True)\
    .save("../outputResult/zipcodes05")