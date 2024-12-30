from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("Alias pyspark example")\
        .master("local[1]")\
        .getOrCreate()

data = [
    ("James","Bond","100",None),
    ("Ann","Varsa","200",'F'),
    ("Tom Cruise","XXX","400",''),
    ("Tom Brand",None,"400",'M')
]
schema = ["fname","lname","id","gender"]

df = spark.createDataFrame(data,schema)

df.printSchema()

## alias() – Set’s name to Column
df.select(df.fname.alias("first_name")).show()

##Another ways
from pyspark.sql.functions import expr
df.select(expr("fname||','||lname")).alias("fullName").show()

##asc and dec --or sorting example

df.sort(df.fname.asc(),df.lname).show()

##cast and astype example for changing datatype

df.select(df.fname,df.id.cast("int")).printSchema()

df.select(df.id.between(100,300)).show()

##contains example

df.select(df.fname.contains("Ann")).show()

### startWith and endWith example

df.filter(df.fname.startswith('T')).show()

df.filter(df.fname.endswith("Cruise")).show()

## isNUll and isNotNull example

df.select(df.lname.isNull().alias("is null")).show()

df.select(df.fname.isNotNull().alias("is not null")).show()

##like and rlike example

df.select(df.fname,df.lname,df.id)\
    .filter(df.fname.like("%m"))

###sub string example
df.select(df.fname.substr(1,2).alias("substr")).show()

###when and otherwise example in SQL switch example

from pyspark.sql.functions import when
df.select(df.fname,df.lname,
          when(df.gender=='M',"Male")\
          .when(df.gender=="F","Female")\
          .when(df.gender==None,"")\
          .otherwise(df.gender).alias("new_gender")
        ).show()


###isin example for check if value present in a list or not

li = ["100","200"]

df.select(df.fname,df.lname,df.id)\
    .filter(df.id.isin(li))\
    .show()


###getField example --to get the value by key from MapType column and by struct child name from structType column

from pyspark.sql.types import StructField,StringType,StructType,ArrayType,MapType

dataNew = [
    (("James","Bond"),["Java","C#"],{"hair":"black","eye":"brown"}),
    (("Ann","Varsa"),[".NET","Python"],{'hair':'brown','eye':'black'}),
    (("Tom Cruise",""),["Python","Scala"],{'hair':'red','eye':'grey'}),
    (("Tom Brand",None),["Perl","Ruby"],{'hair':'black','eye':'blue'})
]

schemaNew = StructType([    
    StructField("name",StructType([
        StructField("fname",StringType(),True),
        StructField("lname",StringType(),True)
    ])),
    StructField("languages",ArrayType(StringType()),True),
    StructField("properties",MapType(StringType(),StringType()),True)
])

df = spark.createDataFrame(data =dataNew,schema = schemaNew)
df.printSchema()
# df.show()

df.select(df.properties.getField("hair").alias("prorName")).show()
df.select(df.name.getField("fname").alias("FullName")).show()

###getItem example

df.select(df.languages.getItem(1).alias("array-value")).show()
df.select(df.properties.getItem("hair")).show()