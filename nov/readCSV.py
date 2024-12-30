from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType

spark = SparkSession.builder\
        .appName("Read csv file from datasource")\
        .master("local[1]")\
        .getOrCreate()

#Read CSV FILE

schema = StructType([
    StructField("DesCountryName", StringType()),
    StructField("OriginCountryName", StringType()), 
    StructField("count", IntegerType())
])

readdata = spark.read.format("csv")\
            .option("header","false")\
            .option("inferSchema","true")\
            .option("schema",schema)\
            .load("../dataSource/2010-summary.csv")

readdata.show(5)
readdata.count()

# print(readdata.collect())

# df = spark.read.csv("../dataSource/2010-summary.csv",header=True,sep="|")

# print(df.collect())