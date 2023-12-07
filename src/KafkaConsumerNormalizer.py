from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# JDBC driver for db connectivity
# jdbc_url = "jdbc:postgresql://your-timescaledb-host:your-port/your-database"
# connection_properties = {
#     "user": "your-username",
#     "password": "your-password",
#     "driver": "org.postgresql.Driver"
# }

# Create a Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Define the Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "test"

# Define the schema for the structured streaming DataFrame
schema = StructType([
    StructField("symbol", StringType()),
    StructField("priceChange", StringType()),
    StructField("priceChangePercent", StringType()),
    StructField("weightedAvgPrice", StringType()),
    StructField("prevClosePrice", StringType()),
    StructField("lastPrice", StringType()),
    StructField("lastQty", StringType()),
    StructField("bidPrice", StringType()),
    StructField("bidQty", StringType()),
    StructField("askPrice", StringType()),
    StructField("askQty", StringType()),
    StructField("openPrice", StringType()),
    StructField("highPrice", StringType()),
    StructField("lowPrice", StringType()),
    StructField("volume", StringType()),
    StructField("quoteVolume", StringType()),
    StructField("openTime", StringType()),
    StructField("closeTime", StringType()),
    StructField("firstId", StringType()),
    StructField("lastId", StringType()),
    StructField("count", StringType())
])

# Define the input DataFrame representing the stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Decode binary data into a string
df = df.withColumn("decoded_value", col("value").cast("string"))

# Parse JSON string into struct
df = df.withColumn("parsed_data", from_json(df["decoded_value"], schema))

# Select individual fields
result_df = df.select(
    "parsed_data.symbol",
    "parsed_data.weightedAvgPrice",
    "parsed_data.prevClosePrice",
    "parsed_data.openPrice",
    "parsed_data.highPrice",
    "parsed_data.lowPrice",
    "parsed_data.volume",
    "parsed_data.quoteVolume",
    "parsed_data.openTime"
)

# filtered_df = result_df.filter(result_df["symbol"].endswith("USDT"))

filtered_df = result_df

# Define a query to print the records to the console
query = filtered_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Await termination of the query
query.awaitTermination()
# spark.stop()