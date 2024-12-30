from pyspark.sql import SparkSession
from os.path import abspath

# .config("spark.sql.warehouse.dir","/user/hive/warehouse")\
# .config("hive.metastore.uris","thrift://localhost:9083/testdb")\
warehouse_location = abspath("/user/hive/warehouse")
spark = SparkSession.builder\
        .appName("Read data from Hive table")\
        .master("local[1]")\
        .enableHiveSupport()\
        .getOrCreate()


#read Hive table

df = spark.read.table("testdb.employee")
df.show()