from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("simpel query example")\
        .master("local[1]")\
        .getOrCreate()


local_file = "../sources/yellow_tripdata_2023-01.parquet"

df = spark.read.parquet(local_file)
df.printSchema()
df.show()

# Query sample from SQL to Spark query:
# select VendorID, total_amount from df
# where total_amount > 1;

df2 = df.select('VendorID','total_amount').where('total_amount > 1')

df2.show(n=5)

#SQL statement

df.createOrReplaceTempView("yellow_taxis")
spark.sql('select count(*) as total_vendor from yellow_taxis where total_amount > 1').show()