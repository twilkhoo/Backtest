from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

spark = SparkSession.builder \
    .appName("KafkaUpperCaseStream") \
    .config(
      "spark.jars.packages",
      "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
    ) \
    .getOrCreate()

# 1) Read the stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "words") \
    .load() \
    .selectExpr("CAST(value AS STRING) AS word")

# 2) Transform to uppercase
upper_df = df.select(upper(col("word")).alias("word_upper"))

# 3) Write out to console
query = upper_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
