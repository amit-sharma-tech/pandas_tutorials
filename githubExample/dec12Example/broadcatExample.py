from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder\
        .appName("Broadcast example")\
        .master("local[1]")\
        .getOrCreate()

df = spark.createDataFrame([
    ('a',2),('b',2),('c',3),('c',44)
],['A','B'])

df.show()

df2 = spark.createDataFrame([('a','aaaa'),('b','bbbb'),('c','cccc')],('A','C'))
     
df.join(df2,df.A == df2.A,'inner').show()

df.join(broadcast(df2),"A").show()

spark.catalog.listDatabases()