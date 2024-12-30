from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Transform example")\
        .master("local[1]")\
        .getOrCreate()

data = [
    ("James,,Smith",["Java","Scala","C++"],["Spark","Java"]),
    ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"]),
    ("Robert,,Williams",["CSharp","VB"],["Spark","Python"])
]

schema = ["name","language1","language2"]

df = spark.createDataFrame(data,schema)

df.printSchema()
df.show()

from pyspark.sql.functions import upper
from pyspark.sql.functions import transform

df.select(transform("language1",lambda x:upper(x)).alias("language1"))\
    .show()