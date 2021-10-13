from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('bigquery')\
    .getOrCreate()

bq_l2 = spark.read\
	.format('bigquery')\
	.option("table", "lesson2.vacancy")\
	.load()

bq_l2.printSchema()
print(f"Count: {bq_l2.count()}")

bq_l2.write\
	.format("parquet")\
	.mode("overwrite")\
	.save("gs://bucket-lesson2/rus_less")