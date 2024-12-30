from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Read data from Mysql DB")\
        .master("local[1]")\
        .config("spark.jars","../jarfile/mysql-connector-java-8.0.11.jar")\
        .getOrCreate()


#create Dataframe
# data = [
#     (1,"Amit"),(2,"John"),(3,"Sandeep")
# ]
# column = ["id","name"]

# df = spark.createDataFrame(data,schema = column)

# df.printSchema()
# df.show()

#read MYSQL DB data

# df  =   spark.read\
#         .format("jdbc")\
#         .option("driver","com.mysql.cj.jdbc.Driver")\
#         .option("url","jdbc:mysql://localhost:3306/classicmodels?useSSL=false")\
#         .option("dbtable","orders")\
#         .option("user","root")\
#         .option("password","admin123")\
#         .load()

# df.printSchema()
# df.show(10)

#load specific column from table

#*** not working

df  =    spark.read.format("jdbc")\
        .option("driver","com.mysql.cj.jdbc.Driver")\
        .option("url","jdbc:mysql://localhost:3306/classicmodels?useSSL=False")\
        .option("dbtable","select * from orders where status='Shipped'")\
        .option("user","root")\
        .option("password","admin123")\
        .load()

df.printSchema()
df.show(10)