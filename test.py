from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([[1,2],[3,4]])
# df.write.csv("test", header=True)
df.show()
#hi