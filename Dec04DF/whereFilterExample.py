from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,ArrayType,IntegerType

spark = SparkSession.builder\
        .appName("Where and filter example")\
        .master("local[1]")\
        .getOrCreate()


data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
]

schema = StructType([
    StructField("name",StructType([
        StructField("fname",StringType(),True),
        StructField("mname",StringType(),True),
        StructField("lname",StringType(),True)
    ])),
    StructField("languages",ArrayType(StringType()),True),
    StructField("state",StringType(),True),
    StructField("gender",StringType(),True)
])

df = spark.createDataFrame(data,schema)

df.printSchema()
df.show()

#Using equal condition

df.filter(df.state=="OH").show()

df.filter("state == 'NY'").show()

from pyspark.sql.functions import col

df.filter(col("state") == "OH").show()