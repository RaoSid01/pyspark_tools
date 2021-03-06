from pyspark.sql.session import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

print('Setting the data table')
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()
print('Initiate to count dataset')
print('Num of records is '+str(df.count()))
print('reprinting the data')
df.select('firstname','lastname').show()
df.select(avg('salary')).show()



print('Code ends here')
spark.stop()
