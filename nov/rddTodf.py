from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
spark = SparkSession.builder\
        .appName("RDD to DF")\
        .master("local[1]")\
        .getOrCreate()

dept = [("Finance",10),("Civil",100),("Sales",300),("IT",400)]

rdd = spark.sparkContext.parallelize(dept)

#CONVERT RDD TO DF

df = rdd.toDF()
df.printSchema()
df.show(truncate=False)


#define column name
deptSchema = ["department Name","dept_id"]

df1 = rdd.toDF(deptSchema)
df1.printSchema()
df1.show()

#Using PySpark createDataFrame() function

deptDF = spark.createDataFrame(rdd,schema = deptSchema)
deptDF.printSchema()
deptDF.show()

#Using createDataFrame() with StructType schema

deptSchema2 = StructType([
        StructField("deptName",StringType(),True),
        StructField("deptId",IntegerType(),True)
])

deptDF2 = spark.createDataFrame(rdd,schema=deptSchema2)

deptDF2.printSchema()
deptDF2.show()