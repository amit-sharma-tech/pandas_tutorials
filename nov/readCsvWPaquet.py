from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType

spark = SparkSession.builder\
        .appName("Parquet file write")\
        .master("local[1]")\
        .getOrCreate()

data = [
    ("James ","","Smith","36636","M",3000),
    ("Michael ","Rose","","40288","M",4000),
    ("Robert ","","Williams","42114","M",4000),
    ("Maria ","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
]

column = ["firstName","middleName","lastName","dob","gender","salary"]

df = spark.createDataFrame(data,schema=column)

df.printSchema()
df.show()

# df.write.parquet("../outputResult/people.parquet")

df.write.mode("overwrite").parquet("../outputResult/people.parquet")

pardf1 = spark.read.parquet("../outputResult/people.parquet")

pardf1.createOrReplaceTempView("peopleTable")

pardf1.printSchema()
pardf1.show(truncate=False)


parkSql = spark.sql("select * from peopleTable where salary >= 4000")
parkSql.show(truncate=False)

spark.sql("create temporary view PERSON using parquet OPTIONS (path\"../outputResult/people.parquet\")")

spark.sql("select * from PERSON").show()

df.write.partitionBy("gender","salary").mode("overwrite").parquet("../outputResult/people.parquet")

pardf2 = spark.read.parquet("../outputResult/people.parquet/gender=M")

pardf2.show(truncate=False)

spark.sql("create temporary view PERSON2 using parquet OPTIONS (path\"../outputResult/people.parquet/gender=M\")")

spark.sql("select * from PERSON2").show()