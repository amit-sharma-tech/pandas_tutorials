from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Flat Map example")\
        .master("local[1]")\
        .getOrCreate()

data =  ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]

rdd = spark.sparkContext.parallelize(data)

for ele in rdd.collect():
    print(ele)

print("=======")
rdd2 = rdd.flatMap(lambda x: x.split(" "))
for ele1 in rdd2.collect():
    print(ele1)

print("=======")

# rdd3 = rdd.flatMap(lambda x: (x,1))
# for ele3 in rdd3.collect():
#     print(ele3)


### explode exmple

from pyspark.sql.functions import explode
arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]

df = spark.createDataFrame(data=arrayData,schema=["name","knowledgel","properties"])

df2 = df.select(df.name,explode(df.knowledgel))
df2.printSchema()
df2.show()