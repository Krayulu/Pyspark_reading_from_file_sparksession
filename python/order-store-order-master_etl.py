from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (SparkSession
    .builder
    .appName("Spark Order store event processing")
    .getOrCreate())

input_path = 'D:\My learnings\PJI\sample\global-order_store_order.csv'
output_path = 'D:\My learnings\PJI\sample\master-order_store_order.csv'

#Input file has following columns:
'''
schema = StructType([
	StructField("insert_timestamp", TimestampType(), True),
	StructField("event_id", StringType(), True),
	StructField("business_date", StringType(), True),
	StructField("store_order_number", StringType(), True),
	StructField("event_type", StringType(), True),
	StructField("business_date_order_taken", StringType(), True),
	StructField("business_time_order_taken", StringType(), True),
	StructField("tax_amount", LongType(), True),
	StructField("discount_amount", LongType(), True),
	StructField("subtotal_amount", LongType(), True),
	StructField("event_timestamp", TimestampType(), True),
	StructField("makeline_print_date", StringType(), True)
])
'''

#Reading the input file to a dataframe
df = spark.read.format("csv").option("header", "true").load(input_path)

#Creating a Temp view table which is binded to current spark session
df.createOrReplaceTempView("global_order_store_order_view")

#Removing Duplicates based on combination of (event_id,event_type) over event_timestamp and creating 'master' data frame
master = spark.sql("SELECT insert_timestamp, a.event_id, business_date, a.store_order_number,a.event_type,business_date_order_taken,tax_amount, discount_amount, subtotal_amount,  event_timestamp, makeline_print_date from (SELECT insert_timestamp, event_id, business_date, store_order_number,event_type,business_date_order_taken,tax_amount, discount_amount, subtotal_amount,  event_timestamp, makeline_print_date, ROW_NUMBER() OVER (PARTITION BY event_id, event_type ORDER BY event_timestamp DESC ) AS index from global_order_store_order_view ) a where a.index=1")

#Writing output single file instead of creating mutiple out files
master.coalesce(1).write.format('com.databricks.spark.csv').save(output_path,header = 'true')
#either obove line over below line can be used
master.coalesce(1).write.csv(output_path,header = 'true')	
