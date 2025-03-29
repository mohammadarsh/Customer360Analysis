from pyspark.sql import  SparkSession
from pyspark.sql.functions import regexp_replace,col

spark = SparkSession.builder.master("local[*]").getOrCreate()

data = [
    (1,'abc#pqr'),
    (2,'pqr@mabo'),
    (3,'nn&ono')
]

df = spark.createDataFrame(data,['id','category'])

df = df.withColumn('category',regexp_replace(col("category"), "[^a-zA-z0-9]",""))

print(df.show())
