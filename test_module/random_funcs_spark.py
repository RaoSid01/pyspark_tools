
from pyspark.sql.functions import lit

def double_data(spark, data):  
    large_data = spark.createDataFrame([], schema= data.schema).withColumn("BATCH", lit(None))
    for i in range(2):
        batch_data = data.withColumn("BATCH", lit(i)+1)
        large_data = large_data.union(batch_data)

    return large_data