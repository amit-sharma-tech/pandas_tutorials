from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Group by example")\
        .master("local[1]")\
        .getOrCreate()

simpleData = [
    ("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
]

column = ["fullName","department","state","salary","age","bonus"]

df = spark.createDataFrame(data=simpleData,schema=column)
df.printSchema()
df.show()

from pyspark.sql.functions import col
df.groupBy(col("department")).sum("salary").show()

df.groupBy("department").min("salary").show()
df.groupBy("department").max("salary").show()
df.groupBy("department").count()

df.groupBy("department","state")\
    .sum("salary","bonus").show()

from pyspark.sql.functions import sum,avg,max

df.groupBy("department")\
    .agg(sum("salary").alias("sum_salary"),\
        avg("salary").alias("avg_salary"),\
        sum("bonus").alias("sum_bonus"),\
        max("bonus").alias("max_bonus")
    ).show()

##filter with agg

df.groupBy("department")\
    .agg(sum("salary").alias("sum_salary"),\
        avg("salary").alias("avg_salary"),\
        sum("bonus").alias("sum_bonus"),\
        max("bonus").alias("max_bonus"))\
    .where(col("sum_bonus") >=5000)\
    .show()

df.createOrReplaceTempView("employees")

sql_string = """
    select department,sum(salary) as sum_salary,
    avg(salary) as avg_salary,
    sum(bonus) as sum_bonus,
    max(bonus) as max_bonus
    from employees group by department having sum(bonus) >50000
"""
print("=======")
df2 = spark.sql(sql_string)
df2.show()