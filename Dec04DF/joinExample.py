from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Join example")\
        .master("local[1]")\
        .getOrCreate()


emp= [
    (1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
    (6,"Brown",2,"2010","50","",-1) \
]

column = ["emp_id","name","suprior_emp_id","year_joined","emp_dept_id","gender","salary"]

empDf = spark.createDataFrame(data=emp,schema=column)

empDf.printSchema()
empDf.show()

dept = [
    ("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
]

deptColumn = ["dept_name","dept_id"]

deptDf = spark.createDataFrame(data=dept,schema=deptColumn)

deptDf.printSchema()
deptDf.show()

###join example

empDf.join(deptDf,empDf.emp_dept_id == deptDf.dept_id,"inner").show()

##left join

empDf.join(deptDf,empDf.emp_dept_id == deptDf.dept_id,"left").show()