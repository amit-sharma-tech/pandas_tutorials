from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType
from pyspark.sql.functions import lit

spark = SparkSession.builder\
        .appName("Column exmple in pyspark")\
        .master("local[1]")\
        .getOrCreate()

colObj = lit("sparkByExample.com")

data = [("James",23),("Ann",40)]

df = spark.createDataFrame(data).toDF("firstName","gender")

# df.printSchema()
# df.show()

####using df object

# df.select(df.gender).show()
# df.select(df['gender']).show()

#####Accessing column name with dot and bracket

# df.select(df["`firstName`"]).show()

#####using SQL col() function

# from pyspark.sql.functions import col

# df.select(col("gender")).show()

# df.select(col("`firstName`")).show()


####Nested example

from pyspark.sql import Row
from pyspark.sql.functions import col
data = [
        Row(name="James",prop=Row(hair="black",eye="blue")),
        Row(name="Ann",prop=Row(hair="grey",eye="black"))
    ]

df = spark.createDataFrame(data)
df.printSchema()
# df.show()

### Select using col and df 

df.select(df.name).show()

df.select(df.prop.hair).show()

df.select(df["prop.eye"]).show()

df.select(col("prop.hair")).show()

df.select(col("prop.*")).show()