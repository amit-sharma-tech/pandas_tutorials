from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType

spark = SparkSession.builder\
        .appName("Empty DataFrame example")\
        .master("local[1]")\
        .getOrCreate()

rdd = spark.sparkContext.parallelize([])
# print(rdd)

#create Empty rdd

rdd1 =  spark.sparkContext.emptyRDD()

# print(rdd)

schema = StructType([
    StructField("firstName",StringType(),True),
    StructField("middleName",StringType(),True),
    StructField("lastName",StringType(),True)
])

df = spark.createDataFrame(rdd,schema)

df.printSchema()
df.show()


df1 = rdd1.toDF(schema)

df1.printSchema()
df1.show()
