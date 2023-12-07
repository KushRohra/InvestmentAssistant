from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, udf
from datetime import datetime, timezone, timedelta
from pyspark.sql.functions import from_json, col
import json

from pyspark.sql.types import StructType, StringType, StructField, DoubleType, BooleanType


##JDBC driver for db connectivity
# jdbc_url = "jdbc:postgresql://your-timescaledb-host:your-port/your-database"
# connection_properties = {
#     "user": "your-username",
#     "password": "your-password",
#     "driver": "org.postgresql.Driver"
# }


## UDFs - For data formatting
def format_timestamp(timestamp_ms):
    timestamp_seconds = timestamp_ms / 1000.0
    dt_utc = datetime.utcfromtimestamp(timestamp_seconds).replace(tzinfo=timezone.utc)
    postgres_timestamp = dt_utc.strftime('%Y-%m-%d %H:%M:%S %z')
    return postgres_timestamp


def parse_json_string(json_string):
    import json
    try:
        data = json.loads(json_string)
        formatted_data = {
            'timestamp': format_timestamp(data['openTime']),
            'symbol': data['symbol'],
            'open': data['openPrice'],
            'high': data['highPrice'],
            'low': data['lowPrice'],
            'close': data['prevClosePrice'],
            'volume_crypto': data['volume'],
            'volume_currency': data['quoteVolume'],
            'weighted_price': data['weightedAvgPrice']
        }
        print(f"json.dumps(formatted_data): {json.dumps(formatted_data)}")
        print(f"type(json.dumps(formatted_data)): {type(json.dumps(formatted_data))}")
        return json.dumps(formatted_data)
    except ValueError:
        return None


def filter_symbol_udf(symbol):
    return symbol.endswith("USDT")


# Create a schema for the structured streaming DataFrame
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("symbol", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume_crypto", DoubleType()),
    StructField("volume_currency", DoubleType()),
    StructField("weighted_price", DoubleType())
])


# Define UDFs
format_timestamp_udf = udf(format_timestamp, StringType())
parse_json_string_udf = udf(parse_json_string, schema)
filter_symbol_udf = udf(filter_symbol_udf, BooleanType())

spark_jars_path = "InvestmentAssistant/spark-sql-kafka-0-10_2.12-3.5.0.jar" 

# Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming")     \
    .config("spark.jars", spark_jars_path) \
    .getOrCreate()


# Define the Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "test"


# Define the input DataFrame representing the stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

decoded_df = df.withColumn("key", col("key").cast("string")).withColumn("value", col("value").cast("string"))

# Apply UDFs
# result_df = result_df.withColumn("timestamp", format_timestamp_udf(result_df["parsed_data"]["timestamp"]))
# filtered_df = result_df.filter(filter_symbol_udf(result_df["parsed_data"]["symbol"]))

query = decoded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() 


# print(f"type(decoded_df.select(col('value').cast('string'))): {type(decoded_df.select(col('value').cast('string')).rdd.map(lambda x: x[0].encode('utf-8')))}")
# print(parse_json_string(decoded_df.select(col("value").cast("string")).rdd.map(lambda x: x[0].encode('utf-8'))))

print(parse_json_string(decoded_df.select(col("value").cast("string"))))


result_df = decoded_df.withColumn("parsed_data", parse_json_string_udf(decoded_df.select(col('value').cast('string'))))
result_df.show()
print(result_df.show())

# print(f"decoded_df['value']: {decoded_df['value'].show()}")
# print(f"type(decoded_df['value']): {type(decoded_df['value'])}")


#Save dataframe to db
# filtered_df.write \
#     .jdbc(url=jdbc_url, table="your-table-name", mode="append", properties=connection_properties)


# Await termination of the query
query.awaitTermination()
spark.stop()
