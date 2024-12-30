from pyspark.sql import SparkSession
from pyspark.sql.functions import col,asc,desc

spark = SparkSession.builder\
        .appName("Acending and descending example")\
        .master("local[1]")\
        .getOrCreate()

simpleData = [
    ("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Raman","Finance","CA",99000,40,24000), \
    ("Scott","Finance","NY",83000,36,19000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
]

columns = ["employee_name","department","state","salary","age","bonus"]

df = spark.createDataFrame(data=simpleData,schema=columns)

df.printSchema()
df.show()

df.sort("department","state").show()
df.sort(col("department"),col("state")).show()


df.orderBy("department","state").show()
df.orderBy(col("department"),col("state")).show()

df.sort(df.department.asc(),df.state.asc()).show()
print("===========")
df.sort(col("department").asc(),col("state").asc()).show()
df.orderBy(col("department").asc(),col("state").asc()).show()


df.sort(df.department.asc(),df.state.desc()).show()
df.sort(col("department").asc(),col("state").desc()).show()
df.orderBy(col("department").asc(),col("state").desc()).show()

###create temp table

df.createOrReplaceTempView("emp")
spark.sql("select employee_name,department,state,salary,age,bonus from emp order by department desc").show()