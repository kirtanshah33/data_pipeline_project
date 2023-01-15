import pyspark
from delta import *

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
## create table
# spark.sql(f"""
# CREATE TABLE orders (
#   OrderId int,
#   OrderStatus varchar(30),
#   LastUpdated timestamp
# )
# USING DELTA
# LOCATION '/opt/application/spark-warehouse/orders'
# """)

## insert data
# spark.sql(
#     """
#     INSERT INTO delta.`/opt/application/delta_tables/orders`
#   VALUES(1,'Backordered',cast( '2020-06-01 12:00:00' as timestamp));
#     """
# )
#df = spark.read.table("default.orders").show()

# read data
# df = spark.read.format("delta").load("/opt/application/delta_tables/orders")
# df.show()

from pyspark.sql.types import StructType, IntegerType, StringType,StructField,TimestampType

schema = StructType([
    StructField("OrderId", IntegerType()),
    StructField("OrderStatus", StringType()),
    StructField("LastUpdated", TimestampType())
])

csv_df = (
    spark.read.format("csv")
    .option("sep", "|")
    .schema(schema)
    .load("/opt/application/order_extract.csv")
)
# csv_df.show()

csv_df.write.format("delta").mode("append").saveAsTable(
    "delta.`/opt/application/spark-warehouse/orders`"
)

df = spark.read.format("delta").load("/opt/application/spark-warehouse/orders")
print(df.printSchema)
df.show()
