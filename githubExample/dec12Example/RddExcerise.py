from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("RDD example")\
        .master("local[1]")\
        .getOrCreate()

filePath= "../data/fakefriends.csv"
lines = spark.sparkContext.textFile(filePath)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age,numFriends)


rdd = lines.map(parseLine)

totalsByage = rdd.mapValues(lambda x :(x,1)).reduceByKey(lambda x, y :(x[0] + y[0],x[1] + y[1]))
averagesByAge = totalsByage.mapValues(lambda x:x[0] / x[1])
results = averagesByAge.collect()

for res in results:
    print(res)

