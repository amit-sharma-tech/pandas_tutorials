from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType

spark = SparkSession.builder\
        .appName("Convert Rdd to Dataframe example")\
        .master("local[1]")\
        .getOrCreate()

dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)


#one ways
# df = rdd.toDF()
# df.printSchema()
# df.show()

#second way

# schema = ["dept_name","dept_id"]

# df = spark.createDataFrame(rdd,schema)

# df.printSchema()
# df.show()

#Using cutomize ways

schema = StructType([
    StructField("dept_name",StringType(),True),
    StructField("dept_id",IntegerType(),True)
])

df = spark.createDataFrame(rdd,schema)

df.printSchema()
df.show()