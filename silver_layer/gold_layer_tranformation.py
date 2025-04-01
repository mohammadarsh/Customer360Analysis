import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.window import  Window
from pyspark.sql.functions import *
from datetime import  datetime,timedelta

from unicodedata import category

from utils import generate_yesterday_date

# intialize argument

parser = argparse.ArgumentParser()
parser.add_argument("--adhoc_date",help="please provide adhoc value")

arg = parser.parse_args()

yesterday_date = generate_yesterday_date(arg.adhoc_date)



print(f"Current Job running date : {yesterday_date}")

spark = SparkSession.builder.master("local[*]").getOrCreate()

customer_df = spark.read.parquet(r"G:\Azure Data Engineering\Lect 13 pyspark Project\Customer360Analsyis\silver_layer\2025-03-24\customer_profile")

trans_df = spark.read.parquet(r"G:\Azure Data Engineering\Lect 13 pyspark Project\Customer360Analsyis\silver_layer\2025-03-24\customer_trasaction")

print(customer_df.printSchema())
print(trans_df.printSchema())

join_df = trans_df.join(customer_df,"customer_id","left")
print(join_df.printSchema())

# Total Spending Per Customer: Aggregates transaction amounts by customer.

win_spec = Window.partitionBy("customer_id")

df = join_df.withColumn("sum_amount",sum(col("amount")).over(win_spec)).drop("amount").distinct()
# print(df.select("customer_id","first_name","last_name","sum_amount").show())
# print(df.select("customer_id","first_name","last_name","sum_amount").count())
print(df.select("customer_id","first_name","last_name","sum_amount").distinct().show())
print(df.select("customer_id","first_name","last_name","sum_amount").distinct().count())

#
# Top Categories by Spending: Identifies the most popular categories
# based on spending.


category_df = join_df.select("category","amount").groupBy("category").agg(sum("amount").alias("total_spent"))

category_df = category_df.orderBy(col("total_spent").desc())
print(category_df.show())


