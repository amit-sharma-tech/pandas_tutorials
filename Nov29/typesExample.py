from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType

spark = SparkSession.builder\
        .appName("StructType and StructField exmaple")\
        .master("local[1]")\
        .getOrCreate()

data = [
    ("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
]

rdd = spark.sparkContext.parallelize(data)

#Create Schema
# schema= ["firstName","middleName","lastName","dob","gender","salary"]

#another ways to create Schema or using StructType and StructFields

schema = StructType([
    StructField("fristName",StringType(),True),
    
])

df = spark.createDataFrame(rdd,schema)

df.printSchema()
df.show()