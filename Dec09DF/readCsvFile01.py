from pyspark.sql import SparkSession

#Create SparkSession

spark = SparkSession.builder\
        .appName("Spark Example for read csv file")\
        .master("local[1]")\
        .getOrCreate()

#read  csv file
filePath = "../dataSource/zipcodes.csv"
# df = spark.read.csv(filePath)
# df.printSchema()
# df.show(5)

#another way to read csv file

# df  =  spark.read.format("csv")\
#         .option("header",True)\
#         .option("inferSchema",True)\
#         .load(filePath)

# df.printSchema()
# df.show(3)

##3rd way to read csv file

schemaType = {
    "header":True,
    "inferSchema":True,
}

# df = spark.read.format("csv")\
#         .options(**schemaType)\
#         .load(filePath)

# df.printSchema()
# df.show(2)

##4 read csv file and save that file in parquet

df = spark.read.format("csv")\
        .options(**schemaType)\
        .load(filePath)

df.printSchema()
df.show(2)

###save that file into parquet file

savepath = "../outputResult/pzipcodes09"
saveFile =  df.write.format("csv")\
             .mode("overwrite")\
             .option("header",True)\
             .parquet(savepath)

##execute SQL of that file

df.createOrReplaceTempView("zipcode")
spark.sql("select * from zipcode limit 2").show()

##read parquet file using temp view
print("=========")
spark.sql("create temp view zipcodes using parquet options(path \"../outputResult/pzipcodes09\")")
spark.sql("select * from zipcodes limit 2").show()
