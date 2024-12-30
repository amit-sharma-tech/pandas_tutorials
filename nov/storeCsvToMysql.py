from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("store csv file data into mysql")\
        .master("local[1]")\
        .config("spark.jars","../jarfile/mysql-connector-java-8.0.11.jar")\
        .getOrCreate()

readCsv =   spark.read.csv("../dataSource/2010-summary.csv",header=True)

# readCsv.printSchema()
# readCsv.show(10)

#store csv data into mysql

readCsv.write.format("jdbc")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("url","jdbc:mysql://localhost:3306/window_func?useSSL=false")\
    .option("dbtable","summary")\
    .option("user","root")\
    .option("password","admin123")\
    .mode("overwrite").save()

spark.stop()
print("load data into table")