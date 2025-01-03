from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Union by name example")\
        .master("local[1]")\
        .getOrCreate()

data = [
    ("James",34), ("Michael",56), \
    ("Robert",30), ("Maria",24)
]

# df1 = spark.createDataFrame(data,schema=["name","id"])
# df1.printSchema()
# df1.show()

# ###create DataFrame df2 with column name and id

# data2 = [(34,"James"),(45,"Maria"), \
#        (45,"Jen"),(34,"Jeff")]

# df2 = spark.createDataFrame(data=data2,schema=["id","name"])
# df2.printSchema()
# df2.show()
# df3 = df1.unionByName(df2)
# df3.printSchema()
# df3.show()

# Using allowMissingColumns
df1 = spark.createDataFrame([[5, 2, 6]], ["col0", "col1", "col2"])
df2 = spark.createDataFrame([[6, 7, 3]], ["col1", "col2", "col3"])
df3 = df1.unionByName(df2, allowMissingColumns=True)
df3.printSchema()
df3.show()