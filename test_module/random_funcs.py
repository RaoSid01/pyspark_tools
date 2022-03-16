from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date

spark = SparkSession.builder.appName('test-app').getOrCreate()

def replicate_data(n, data): 
    num_batches = n
    large_data = spark.createDataFrame([], schema= data.schema).withColumn("BATCH", lit(None))
    for i in range(num_batches):
        batch_data = data.withColumn("BATCH", lit(i))
        large_data = large_data.union(batch_data)

    return large_data