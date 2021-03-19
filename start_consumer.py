from pyspark.sql import *
from pyspark.sql.types import *
from kafka import KafkaConsumer
from pyspark.context import SparkContext


spark = SparkSession.builder.appName("Spark Structured Streaming from Kafka").getOrCreate()
sdfMarket = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "pi-node11:9092")\
    .option("subscribe", "market")\
    .option("startingOffsets", "latest")\
    .load()\
    .selectExpr("CAST(value AS STRING)")

sdfMarket.printSchema()

marketSchema = StructType([\
    StructField("order_quentity", LongType()), StructField("trade_type", StringType()),\
    StructField("symbol", StringType()), StructField("timestamp", TimestampType()),\
    StructField("bid_price", FloatType())])

def parse_data_from_kafka_message(sdf, schema):
    from pyspark.sql.functions import split
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    col = split(sdf['message'], ',') #split attributes to nested array in one Column
    #now expand col to multiple top-level columns 
    for idx, field in enumerate(schema):
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in schema])

sdfMarket = parse_data_from_kafka_message(sdfMarket, marketSchema)

print(sdfMarket)
