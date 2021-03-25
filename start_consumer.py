from pyspark.sql import *
from pyspark.sql.types import *
import kafka
from kafka import KafkaConsumer
from pyspark.context import SparkContext
from pyspark.sql.functions import split, col, from_json, to_json, window


spark = SparkSession.builder.appName("Spark Structured Streaming from Kafka").getOrCreate()

sdfMarket = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "market")\
    .option("startingOffsets", "latest")\
    .load()\
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


marketSchema = StructType([\
    StructField("order_quantity", LongType()), StructField("trade_type", StringType()),\
    StructField("symbol", StringType()), StructField("timestamp", TimestampType()),\
    StructField("bid_price", FloatType())])

nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
jsonOptions = { "timestampFormat": nestTimestampFormat }

a = sdfMarket.select(from_json("value", marketSchema).getItem("order_quantity").alias("order_quantity"), \
                     from_json("value", marketSchema).getItem("trade_type").alias("trade_type"), \
                     from_json("value", marketSchema).getItem("symbol").alias("symbol"), \
                     from_json("value", marketSchema).getItem("timestamp").alias("timestamp"), \
                     from_json("value", marketSchema).getItem("bid_price").alias("bid_price"))


################ Traitement des données #############################


# Finding the average bid price per symbol using a time window of 10 seconds.

query1 = a.withWatermark("timestamp", "10 seconds").groupBy("symbol", window("timestamp", "10 seconds")).avg("bid_price")

query1.writeStream \
.outputMode("append") \
.format("console") \
.start() \
.awaitTermination()
