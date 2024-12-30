from pyspark.sql import SparkSession
from pyspark.sql import functions as sqlfun
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,TimestampType

spark = SparkSession.builder\
        .appName("spark streaming example")\
        .master("local[1]")\
        .getOrCreate()

local_file = "../sources/sample_employee.csv"

emp_schema = StructType([
    StructField("employee_id",IntegerType(),True),
    StructField("department_name",StringType(),True),
    StructField("name",StringType(),True),
    StructField("last_name",StringType(),True),
    StructField("hire_timestamp",TimestampType(),True)
])

# df = spark.read.csv(local_file)
df = spark.read.format("csv")\
    .schema(emp_schema)\
    .option("header",True)\
    .option("inferScehma",True)\
    .option("maxFilesPerTrigger",1)\
    .load(local_file)
# df.printSchema()

#is my stream activited 
df.isStreaming

##show schema

df.printSchema()

##Add aggregation

df_large_teams = df.withWatermark("hire_timestamp", "10 Minutes")\
                .groupBy("department_name",'hire_timestamp')\
                .agg((sqlfun.count('employee_id').alias('count')),sqlfun.max("hire_timestamp"))\
                .where('count > 1')

df_stream_teams = df_large_teams.writeStream.format('console').outputMode('complete').start()

#append streamed data to storage

output_location = "../output/dept_employee/large_depts"

df_stream_large_teams = df_large_teams.writeStream\
                        .format("csv")\
                        .outputMode("append")\
                        .option("path",output_location)\
                        .start()

df_stream_large_teams.stop()

