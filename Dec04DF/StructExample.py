from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType

spark = SparkSession.builder\
        .appName("StructType and StructField example")\
        .master("local[1]")\
        .getOrCreate()

data = [
    ("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
]

# schema = StructType([
#     StructField("firstName",StringType(),True),
#     StructField("middleName",StringType(),True),
#     StructField("lastName",StringType(),True),
#     StructField("id",StringType(),True),
#     StructField("gender",StringType(),True),
#     StructField("salary",IntegerType(),True)
# ])

# df = spark.createDataFrame(data,schema)

# df.printSchema()
# df.show()

### Another  ways with parallelize 

rdd = spark.sparkContext.parallelize(data)

schema2 = ["firstName","middleName","lastName","id","gender","salary"]

df2 = rdd.toDF(schema2)

df2.printSchema()
df2.show()

# print(df2)
