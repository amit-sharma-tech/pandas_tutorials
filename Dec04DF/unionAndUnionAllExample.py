from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Union Example")\
        .master("local[1]")\
        .getOrCreate()


data = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

column = ["fullName","department","state","salary","age","bonus"]

df = spark.createDataFrame(data,schema=column)

df.printSchema()
df.show()


simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
]

column2 = ["fname","dept","state","salary","age","bonus"]

df2 = spark.createDataFrame(data=simpleData2,schema=column2)
df2.printSchema()
df2.show()


unionDF = df.union(df2)

print(f"total uniona count : {str(unionDF.count())}")
unionDF.show()


unionAll = df.unionAll(df2)

print(f"total unionall count : {str(unionAll.count())}")
unionAll.show()


###uion with distinct example

distDf = df.union(df2).distinct()

print(f"distinct count : {str(distDf.count())}")
distDf.show()


