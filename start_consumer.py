from pyspark.sql import *
from pyspark.sql.types import *
import kafka
from kafka import KafkaConsumer
from pyspark.context import SparkContext
from pyspark.sql.functions import split, col, from_json, to_json


#print("version", kafka.__version__)

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

#a = sdfMarket.select( col("key").cast("string"), from_json(col("value").cast("string"), marketSchema, jsonOptions).alias("parsed_value") )
a = sdfMarket.select(from_json("value", marketSchema).getItem("order_quantity").alias("order_quantity"), \
                     from_json("value", marketSchema).getItem("trade_type").alias("trade_type"), \
                     from_json("value", marketSchema).getItem("symbol").alias("symbol"), \
                     from_json("value", marketSchema).getItem("timestamp").alias("timestamp"), \
                     from_json("value", marketSchema).getItem("bid_price").alias("bid_price"))

# query = a.groupBy("symbol").count()
a.writeStream \
.outputMode("append") \
.format("console") \
.start() \
.awaitTermination()
