import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType
from pyspark.sql.types import ArrayType,DoubleType,BooleanType
from pyspark.sql.functions import col,array_contains

spark = SparkSession.builder\
        .appName("Full read csv file")\
        .master("local[1]")\
        .getOrCreate()

df = spark.read.csv("../dataSource/zipcodes.csv")

df.printSchema()

df2 = spark.read.option("header",True)\
    .csv("../dataSource/zipcodes.csv")

df2.printSchema()

df3 = spark.read.options(header=True,delimiter=',')\
    .csv("../dataSource/zipcodes.csv")

df3.printSchema()

df4 = spark.read.options(header=True,delimiter=',',inferSchema=True)\
    .csv("../dataSource/zipcodes.csv")

df4.printSchema()

scehma = StructType([
    StructField("RecordNumber",IntegerType(),True),
    StructField("Zipcode",IntegerType(),True),
    StructField("ZipCodeType",StringType(),True),
    StructField("City",StringType(),True),
    StructField("State",StringType(),True),
    StructField("LocationType",StringType(),True),
    StructField("Lat",DoubleType(),True),
    StructField("Long",DoubleType(),True),
    StructField("Xaxis",DoubleType(),True),
    StructField("Yaxis",DoubleType(),True),
    StructField("Zaxis",DoubleType(),True),
    StructField("WorldRegion",StringType(),True),
    StructField("Country",StringType(),True),
    StructField("LocationText",StringType(),True),
    StructField("Location",StringType(),True),
    StructField("Decommisioned",BooleanType(),True),
    StructField("TaxReturnField",IntegerType(),True),
    StructField("EstimatedPopulation",IntegerType(),True),
    StructField("TotalWages",IntegerType(),True),
    StructField("Notes_val",StringType(),True)
])
df_with_schema = spark.read.format("csv")\
                    .option("header",True)\
                    .schema(scehma)\
                    .load("../dataSource/zipcodes.csv")

df_with_schema.printSchema()

df_with_schema.show(10)
