
from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Example for When build IN function")\
        .master("local[1]")\
        .getOrCreate()


data = [
    ("James","M",60000),("Michael","M",70000),
    ("Robert",None,400000),("Maria","F",500000),
    ("Jen","",None)
]

columns=  ["firstName","gender","amount"]

df = spark.createDataFrame(data,schema=columns)

# df.show()

# from pyspark.sql.functions import col,when

# df2 = df.withColumn("new_gender",
#         when(df.gender == "M","Male")
#         .when(col('gender') == "F","Female")
#         .when(df['gender'].isNull() ,"")
#         .otherwise(df['gender'])
#         )

# df2.show()

##same above code convert into SQL

from pyspark.sql.functions import expr,col

#using Case When on withColumn()

# df3 = df.withColumn("new_gender",
#         expr("CASE WHEN gender = 'M' THEN 'Male'" +
#         "WHEN gender ='F' THEN 'Female'"+
#         "WHEN gender IS NULL THEN ''"+
#         "ELSE gender END"
#         ))

# df3.show()

#Using Case When on select()

# df4= df.select(col("*"), expr("CASE WHEN gender ='M' THEN 'Male'"+
#                               "WHEN gender ='F' THEN 'Female'"+
#                               "WHEN gender IS NULL THEN ''"+
#                               "ELSE gender END"
#                             ).alias("new_gender"))

# df4.show()

##Using case when on SQL expression

df.createOrReplaceTempView("emp")

spark.sql("select firstName,CASE WHEN gender='M' THEN 'Male'"+
          "WHEN gender ='F' THEN 'Female'"+
          "WHEN gender IS NULL THEN''"+
          "ELSE gender END as new_gender from emp"
          ).show()