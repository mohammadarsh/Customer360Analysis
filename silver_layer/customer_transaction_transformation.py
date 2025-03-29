import argparse

from pyspark.sql import SparkSession
from pyspark.sql.window import  Window
from pyspark.sql.functions import *
from datetime import  datetime,timedelta


#intialize argument

parser = argparse.ArgumentParser()
parser.add_argument("--adhoc_date",help="please provide adhoc value")

arg = parser.parse_args()


#get Yesterday's date

if arg.adhoc_date:
    yesterday_date = arg.adhoc_date
else:
    yesterday_date = datetime.now() - timedelta(days=1)
    yesterday_date = yesterday_date.strftime("%Y-%m-%d")

print(f"Current Job running date : {yesterday_date}")



spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.read.csv(fr"G:\Azure Data Engineering\Lect 13 pyspark Project\Customer360Analsyis\bronze_layer\customer_transaction\{yesterday_date}\customer_transaction.csv",header=True,inferSchema=True)

print(df.printSchema())



#Remove Duplicates

window_spec = Window.partitionBy("transaction_date","transaction_id","customer_id").orderBy("transaction_date")

df = df.withColumn("row_num",row_number().over(window_spec))

df = df.filter("row_num = 1").drop("row_num")

# print(df.show())

# transformation 2: Clean invalid or missing values.

df = df.withColumn('category',regexp_replace(col("category"), "[^a-zA-z0-9 ]",""))

# df.show()

#Transformation 3 : is_large_transaction for transactions > $1000.

df = df.withColumn("is_large_transaction",
                   when(col("amount")>1000,True).otherwise(False))

# print(df.filter("is_large_transaction")=="False")


#tranformation 4: Get year, month, day for easier aggregation

df = df.withColumn("transaction_date",to_timestamp(col("transaction_date")))
df = df.withColumn("transaction_year",year(col("transaction_date"))).withColumn("transaction_month",month(col("transaction_date"))).withColumn("transaction_days",dayofmonth(col("transaction_date")))
print(df.select("transaction_year","transaction_month","transaction_days").distinct().show())

#write

df.write.mode("overwrite").parquet(fr"G:\Azure Data Engineering\Lect 13 pyspark Project\Customer360Analsyis\silver_layer\{yesterday_date}\customer_trasaction")
