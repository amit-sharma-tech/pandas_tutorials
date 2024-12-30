from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("translate example")\
        .master("local[1]")\
        .getOrCreate()

address = [(1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA")]

df =spark.createDataFrame(address,["id","address","state"])
df.show()

#Replace string
from pyspark.sql.functions import regexp_replace
df.withColumn('address', regexp_replace('address', 'Siemon', 'Road121')) \
  .show(truncate=False)
