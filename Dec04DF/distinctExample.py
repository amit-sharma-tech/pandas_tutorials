from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder\
        .appName("Distinct example")\
        .master("local[1]")\
        .getOrCreate()

data = [
    ("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
]

column = ["employee_name","department","salary"]

df = spark.createDataFrame(data,schema=column)

df.printSchema()
df.show()
print("colut all :"+ str(df.count()))
distinctData = df.distinct()
print("Distinct count:" + str(distinctData.count()))
distinctData.show()

df2 = df.dropDuplicates()
print("Distinct count: "+str(df2.count()))
print(f"Distinct count:{str(df2.count())}")