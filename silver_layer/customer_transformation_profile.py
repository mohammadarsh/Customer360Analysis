import argparse
from multiprocessing.reduction import duplicate

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.window import  Window
from pyspark.sql.functions import *
from datetime import  datetime,timedelta


from utils import generate_yesterday_date

# intialize argument

parser = argparse.ArgumentParser()
parser.add_argument("--adhoc_date",help="please provide adhoc value")

arg = parser.parse_args()

yesterday_date = generate_yesterday_date(arg.adhoc_date)



print(f"Current Job running date : {yesterday_date}")

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.read.csv(r"G:\Azure Data Engineering\Lect 13 pyspark Project\Customer360Analsyis\bronze_layer\customer_profile\2025-03-24\customer_profile.csv",header=True)

print(df.printSchema())

#Remove Duplicate
win_spec = Window.partitionBy("signup_date","customer_id").orderBy("signup_date")

df = df.withColumn("row_num",row_number().over(win_spec))

#duplicatie or bad Store
duplicate_bad_df  = df.filter("row_num !=1").drop("row_num")
print(duplicate_bad_df.show())
#get Unique Records
df = df.filter("row_num=1").drop("row_num")

print(df.show())
#While validating email, consider email domain ends with “.com” and “.net”

email_bad_df = df.filter((~col("email").endswith('.com')) | ~col("email").endswith('.net'))

df = df.filter((col("email").endswith(".com")) | col("email").endswith(".net"))

# print(df.select("email").show(truncate=False))
print(email_bad_df.select("email").show(truncate=False))
#Evalute or Extract year day month or we can use to_date function also
df = df.withColumn("signup_year",year(col("signup_date").cast(DateType()))).withColumn("signup_month",month(col("signup_date").cast(DateType()))).withColumn("signup_day",dayofmonth(col("signup_date").cast(DateType())))
print(df.select("signup_date","signup_year","signup_month","signup_day").distinct().show())


#Write Mode
df.write.mode("overwrite").parquet(fr"G:\Azure Data Engineering\Lect 13 pyspark Project\Customer360Analsyis\silver_layer\{yesterday_date}\customer_profile")

#duplicate records store

duplicate_bad_df.union(email_bad_df).coalesce(1).write.mode("overwrite").csv(fr"G:\Azure Data Engineering\Lect 13 pyspark Project\Customer360Analsyis\silver_layer\bad_records\{yesterday_date}\customer_profile",header=True)