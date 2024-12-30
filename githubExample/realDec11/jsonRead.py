import logging
from pyspark.sql import SparkSession

def rdd_to_dataFrame(data,schema):
    #Create a sparkSession

    spark = SparkSession.builder\
            .appName("SQL example")\
            .master("local[1]")\
            .getOrCreate()
    try:
        # end try
        rdd = spark.sparkContext.parallelize(data)

        #convert RDD to DataFrame
        df = spark.createDataFrame(rdd,schema)

        #return the dataframe,without stopping the sparksession

        return df
    except Exception as e:
        logging.error('Error while transforming RDD to DF :{}'.format())
        spark.stop()
        # raise e


# Data sample
dept_data = [(1,"Big Data"), (2, "Finance"), (3,"Marketing")]
dept_schema = ["department_id", "department_name"]
# Data sample
emp_data = [(1,"Carlos", 17), (1,"Bob", 30), (2,"Jasmin", 26),(3,"Nishi", 36)]
emp_schema = ["department_id","employee_name", "age"]

# Call function, to transform RDD into DF
df_emp = rdd_to_dataFrame(emp_data, emp_schema)
df_dept = rdd_to_dataFrame(dept_data, dept_schema)

# Register as view
df_emp.createOrReplaceTempView('employees')
df_dept.createOrReplaceTempView('departments')

##read json file

spark = SparkSession.builder\
            .appName("SQL example")\
            .master("local[1]")\
            .getOrCreate()

# Let's now save the JOINED RESULTSET into a new Temporary View -- NO WHERE CLAUSE
showDf = spark.sql('''
        select emp.employee_name, emp.age, emp.department_id, dept.department_name
        from employees as emp
            inner join departments as dept on (emp.department_id = dept.department_id)
        ''').createOrReplaceTempView('dept_employees')

spark.sql("select * from dept_employees").show()



load_file = "../sources/department_budget.json"

df_budget = spark.read.format("json")\
            .option("multiline",True)\
            .load(load_file)

df_budget.printSchema()
df_budget.show()   

#we can still query data using json path

df_budget.select("offices").where('department_id == 1').show(truncate=False)

##Let's join the thord datasets

df_budget.createOrReplaceTempView('budgets_json')

spark.sql('''
        select
        e.employee_name,
        e.department_name,
        e.department_id,
        b.budget,
        b.budget_period,
        b.offices[0].cost_center.office as office_1,
        b.offices[0].cost_center.budget_status as budget_status_1,
        b.offices[1].cost_center.office as office_2,
        b.offices[1].cost_center.budget_status as budget_status_2,
        b.offices[2].cost_center.office as office_3,
        b.offices[2].cost_center.budget_status as budget_status_3,
        nvl(b.budget_authorizer[0].cto.name, "no CTO registed") as cto_name,
        nvl(b.budget_authorizer[0].cto.last_name ,"no CTO registed") as cto_last_name
        from
        dept_employees as e
        inner join 
        budgets_json as b
        on (e.department_id = b.department_id)
    ''').show()

##filter JSON column example

from pyspark.sql.types import StructField,StructType,StringType,IntegerType
from pyspark.sql.functions import col,explode_outer


def flatten_dataFrame(df):
    try:
        complex_fields = dict([]) 
    except Exception as e:
        raise e
    # end try