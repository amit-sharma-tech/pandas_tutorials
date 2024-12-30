from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("All State isurance Amount predication example")\
        .config('spark.sql.debug.maxToStringFields', 2000) \
        .config('spark.debug.maxToStringFields', 2000) \
        .master("local[15]")\
        .getOrCreate()


trainDatapath = "../data/train.csv"
testDataPath = "../data/test.csv"

train_data = spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema",True)\
            .load(trainDatapath)

test_data = spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema",True)\
            .load(testDataPath)

# train_data.printSchema()
# train_data.show(5)

# train_data.count()

# train_data = train_data.withColumnRenamed("loss","label")

# train_data.printSchema()

[trainingData,validationData] = train_data.randomSplit([0.7,0.3])

trainingData.cache()
validationData.cache()

columns = train_data.columns

#Get all the categorial cols 
# print(list(columns))

# for item in columns:
#     print(f'columnd name :- {item} \n')

cat_cols = filter(lambda s :s.startswith("cat"),columns)

# print(list(cat_cols))

# count_cols = list(filter(lambda s : s.startswith("cont"),columns))

# print(count_cols)

###create list of stringindexer example

stringIndexList = []

import pyspark.ml.feature as ft

for col in cat_cols:
    stringIndexList.append(ft.StringIndexer(inputCol=col,outputCol='idx'+col[3:]))
    # print(str(ft.StringIndexer(inputCol=col,outputCol='idx'+col[3:])) + "\n")

# a = stringIndexList[3]
# print(a.getOutputCol())

# print(len(stringIndexList))

idx_cat_cols = map(lambda s :s.replace('cat', 'idx'),cat_cols)

print(list(idx_cat_cols))