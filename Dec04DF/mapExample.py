from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Map example")\
        .master("local[1]")\
        .getOrCreate()

data = ["Project","Gutenberg’s","Alice’s","Adventures",
"in","Wonderland","Project","Gutenberg’s","Adventures",
"in","Wonderland","Project","Gutenberg’s"]

rdd = spark.sparkContext.parallelize(data)

# # print(rdd)

# rdd2 = rdd.map(lambda x:(x,1))
# for ele in rdd2.collect():
#     print(ele)

rdd2 = rdd.map(lambda x:(x,1))
for ele in rdd2.collect():
    print(ele)

data = [('James','Smith','M',30),
  ('Anna','Rose','F',41),
  ('Robert','Williams','M',62), 
]

columns = ["firstName","lastName","gender","salary"]

df = spark.createDataFrame(data,schema=columns)

df.show()

# rdd2 = df.rdd.map(lambda x:(x[0]+ ","+x[1],x[2],x[3]))
# df2 = rdd2.toDF(["name","gender","new_salary"])
# df2.show()

#Referring Column Names

def func1(x):
    firstName = x.firstName
    lastName = x.lastName
    name = firstName + ", " + lastName
    gender = x.gender.lower()
    salary = x.salary*2
    return (name,gender,salary)

rdd2 = df.rdd.map(lambda x:func1(x))

df2  = rdd2.toDF()
df2.show()