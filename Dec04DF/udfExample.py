from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("UDF example")\
        .master("local[1]")\
        .getOrCreate()

data =  [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

column = ["seqno","Name"]

df = spark.createDataFrame(data,schema=column)

df.show()

def convertCase(str):
    resStr = ""
    arr = str.split(" ")
    for x in arr:
        resStr = resStr + x[0:1].upper() + x[1:len(x)]+ " "
    return resStr

from pyspark.sql.functions import col,udf
from pyspark.sql.types import StringType

converUdf = udf(lambda z: convertCase(z),StringType())

print(list(converUdf))


