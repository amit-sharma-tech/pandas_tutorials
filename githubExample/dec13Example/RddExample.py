from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Rdd Example")\
        .master("local[1]")\
        .getOrCreate()

sc = spark.sparkContext.parallelize([1,2,3,4,5],2)

print(sc.collect())

babyFilePath = "../data/Baby_Names__Beginning_2007.csv"

lines = spark.sparkContext.textFile(babyFilePath)

#print the first element
print(lines.first())

#returns first 5 elements
print(lines.take(5))

print("\n")
#returning of first 5 elelment
print(lines.map(lambda x : (x)).take(5))

#return total number of characters

rdd = lines.map(lambda s: len(s))
