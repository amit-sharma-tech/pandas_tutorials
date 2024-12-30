import logging

from pyspark.sql import SparkSession

def rdd_to_dataFrame(data,schema):
    try:
        #create an RDD from the input data ,using data context not session
        rdd = spark.sparkContext.parallelize(data)

        #convert RDD to dataFrame

        df = spark.createDataFrame(rdd,schema)

        #retrun the DF without stopping the sparkSession

        return df
    except Exception as e:
        #Log error and stop the SparkSession
        logging.error('Error while transforming RDD to DF :{}'.format(e))
        spark.stop()
        # raise e
    # end try

spark = SparkSession.builder\
        .appName("RDDtoDataFrame")\
        .master("local[1]")\
        .getOrCreate()

#Create Data
#Data dept sample

dept_data = [(1,"Big Data"),(2,"Finance"),(3,"marketing")]
dept_schema = ["department_id","department_name"]


#data emp sample

emp_data = [(1,"carlos",17),(1,"Bob",30),(2,"Jasmin",26)]

emp_schema = ["department_id","employee_name","age"]


#calling funciton

df_dept = rdd_to_dataFrame(dept_data,dept_schema)

df_emp = rdd_to_dataFrame(emp_data,emp_schema)

#printing Schema

df_dept.printSchema()
df_emp.printSchema()

#showing data
df_dept.show()
df_emp.show()


#Register as view

df_dept.createOrReplaceTempView("departments")
df_emp.createOrReplaceTempView("employee")

#query sample using spark SQL

# spark.sql('''
#             select e.*,d.* 
#             from employee as e
#             inner join departments as d
#             on (e.department_id = d.department_id)
#             where age  >18
#           ''').show()

#lets now save the joined resultset into a new temp view 

spark.sql('''
        select e.department_id,e.employee_name,e.age,d.department_name
        from employee as e
        inner join departments as d
        on e.department_id = d.department_id
        where e.age >=18
        ''').createOrReplaceTempView('dept_emp')

spark.sql('''
        select * from dept_emp where department_id is not null
          ''').show()

##Define output location

output_location = "../output/dept_employee"

spark.sql('''
        select * from dept_emp where department_id is not null
          ''').write.mode('overwrite').csv(output_location)

spark.stop