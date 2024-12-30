from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Pivot example")\
        .master("local[1]")\
        .getOrCreate()


data = [
    ("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")
]

column =  ["product","Amount","Country"]

df = spark.createDataFrame(data,schema=column)

df.printSchema()
df.show()

countries = ["USA","China","Canada","Maxico"]

pivotDF = df.groupBy("Product").pivot("Country",countries).sum("Amount")
pivotDF.show()