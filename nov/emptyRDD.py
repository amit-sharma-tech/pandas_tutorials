from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Empty spark create")\
        .master("local[1]")\
        .getOrCreate()

#create EMPTY RDD in pyspark
# print(spark)
# emptyRdd = spark.sparkContext.emptyRDD()
# print(emptyRdd)

# emptyRdd2 = spark.sparkContext.parallelize([])
# print(emptyRdd2)

#CREATE EMPTY RDD USING DATAFRAME WITH SCHEMA(STRUCT-TYPE)

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

schema = StructType([
    StructField("firstName",StringType(),True),
    StructField("middleName",StringType(),True),
    StructField("lastName",StringType(),True),
])

emptyRdd = spark.sparkContext.parallelize([])

df = spark.createDataFrame(emptyRdd,schema)

print(df.printSchema())

#CONVERT EMPTY RDD TO DATAFRAME

df1 = emptyRdd.toDF(schema)
print(df1)

#CREATE EMPTY DATAFRAME WITH SCHEMA

df2 = spark.createDataFrame([],schema)
print(df2)


df3 = spark.createDataFrame(emptyRdd,StructType([]))
print(df3)
