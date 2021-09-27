from pyspark.sql import SparkSession
from time import sleep
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder.appName("SparkStreamingKafka").getOrCreate()

schema = StructType().add("id",IntegerType()).add("action", StringType())
users_schema = StructType().add("id",IntegerType()).add("user_name", StringType()).add("user_age", IntegerType())

users_data = [(1,"Jimmy",18),(2,"Hank",48),(3,"Johnny",9),(4,"Erle",40)]
users = spark.createDataFrame(data=users_data,schema=users_schema)
users.repartition(1).write.csv("static/users","overwrite",header=True)

input_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "net-topic") \
  .option("failOnDataLoss", False) \
  .load()

json_stream = input_stream.select(col("timestamp").cast("string"), from_json(col("value").cast("string"), schema).alias("parsed_value"))

clean_data = json_stream.select(col("timestamp"), col("parsed_value.id").alias("id"), col("parsed_value.action").alias("action"))

# join_stream = clean_data.join(users, clean_data.id == users.id, "left_outer").select(users.user_name, users.user_age, col('count'))
join_stream = clean_data.join(users, clean_data.id == users.id, "left_outer").select(clean_data.timestamp, users.user_name, users.user_age)

join_stream.writeStream.format("console").outputMode("append").start().awaitTermination()