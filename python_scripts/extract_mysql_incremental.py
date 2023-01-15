import pymysql
import csv
import configparser
import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

last_record_sql = """SELECT COALESCE(MAX(LastUpdated), '1900-01-01')
    FROM delta.`/opt/application/delta_tables/orders`;"""
result = spark.sql(last_record_sql).first()
# # there's only one row and column returned
last_updated_warehouse = result[0]


# get the MySQL connection info and connect
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password")

conn = pymysql.connect(host=hostname,
        user=username,
        password=password,
        db=dbname,
        port=int(port))

if conn is None:
  print("Error connecting to the MySQL database")
else:
  print("MySQL connection established!")

m_query = """SELECT *
    FROM Orders
    WHERE LastUpdated > %s;"""
local_filename = "order_extract.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query, (last_updated_warehouse,))
results = m_cursor.fetchall()

with open(local_filename, 'w') as fp:
  csv_w = csv.writer(fp, delimiter='|')
  csv_w.writerows(results)

fp.close()
m_cursor.close()
conn.close()


# # load the aws_boto_credentials values
# parser = configparser.ConfigParser()
# parser.read("pipeline.conf")
# access_key = parser.get(
#     "aws_boto_credentials",
#     "access_key")
# secret_key = parser.get(
#     "aws_boto_credentials",
#     "secret_key")
# bucket_name = parser.get(
#     "aws_boto_credentials",
#     "bucket_name")

# s3 = boto3.client(
#     's3',
#     aws_access_key_id=access_key,
#     aws_secret_access_key=secret_key)

# s3_file = local_filename

# s3.upload_file(
#     local_filename,
#     bucket_name,
#     s3_file)
