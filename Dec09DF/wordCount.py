from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Word count problem")\
        .master("local[1]")\
        .getOrCreate()


filepath = "../dataSource/word.txt"
textFile = spark.sparkContext.textFile(filepath)

print(type(textFile))

#word list

word = textFile.flatMap(lambda line: line.split(" "))
# print(word.collect())

not_empty = word.filter(lambda x :x !="")

key_values = not_empty.map(lambda li :(li,1))
print((key_values))
print("\n")
counts = key_values.reduceByKey(lambda a,b :a +b)

# print(f"key_values :- ${key_values} \n count value :- ${counts.collect()}")

show_all = counts.collect()

for(word,count) in show_all:
    print("%s:%i" %(word,count))

