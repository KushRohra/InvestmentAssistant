from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import col

from pyspark.sql.functions import dayofweek, hour
from pyspark.sql.functions import lag
from pyspark.sql import functions as F

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.ml.feature import StringIndexer, OneHotEncoder

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.regression import GBTRegressor

from pyspark.sql.window import Window

import numpy as np
import pandas as pd

spark = SparkSession.builder.appName("BitcoinDataProcessing").getOrCreate()

db_url = "jdbc:postgresql://localhost:5432/mydatabase"
db_properties = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "org.postgresql.Driver"
}
# jdbc_driver_path = "../jars/postgresql-42.7.1.jar"
# spark.sparkContext.addFile(jdbc_driver_path)
# db_properties["driver"] = "org.postgresql.Driver"
table_name = "crypto_price_data"
bitcoin_data_full = spark.read \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", table_name) \
    .option("user", db_properties['user']) \
    .option("password", db_properties['password']) \
    .option("driver", db_properties['driver']) \
    .load()
# bitcoin_data_full = spark.read.csv("./bitcoin_historic_data.csv", header=True, inferSchema=True)

bitcoin_data = bitcoin_data_full
print(f"bitcoin_data_full.filter(col('close').isNull()).count(): {bitcoin_data_full.filter(col('close').isNull()).count()}")
# print(bitcoin_data_full.count())
print(f"bitcoin_data.count(): {bitcoin_data.count()}")
print(type(bitcoin_data))


# Converting the timestamp from unixtimestamp to a readable format
bitcoin_data = bitcoin_data.withColumn("Timestamp", bitcoin_data["Timestamp"].cast("timestamp"))

# Extracting the date part from the timestamp
bitcoin_data = bitcoin_data.withColumn("Date", F.to_date("Timestamp"))

# Define a window specification partitioned by date and ordered by timestamp
window_spec = Window.partitionBy("Date").orderBy("Timestamp")

# Getting the first record of each day
# bitcoin_data = bitcoin_data.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")

# print(bitcoin_data.toPandas()[['Close']].isnull().sum(
# .0+), bitcoin_data.toPandas()[['Close']].count())


from pyspark.sql.functions import col, when, last
from pyspark.sql.window import Window

other_columns = ["symbol", "weighted_price", "volume_crypto", "volume_currency"]
for col_name in other_columns:
#     bitcoin_data = bitcoin_data.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
    bitcoin_data = bitcoin_data.na.fill(value=0,subset=[col_name])

# Fill forward for OHLC data
# ohlc_columns = ["Open", "High", "Low", "Close"]
# for col_name in ohlc_columns:
#     # Create a window specification to fill forward based on timestamp
#     window_spec = Window.orderBy("Timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)

#     # Fill forward using the last non-null value
#     bitcoin_data = bitcoin_data.withColumn(col_name, last(col_name, ignorenulls=True).over(window_spec))

# Fill forward for OHLC data    
# ohlc_columns = ["Open", "High", "Low", "Close"]
# # bitcoin_data_pd = bitcoin_data.toPandas() # Converting to pandas df since Spark function not working as expected 
# for col_name in ohlc_columns:
#     windowSpec = Window.orderBy(F.col('timestamp'))
#     bitcoin_data = bitcoin_data.withColumn(
#         col_name,
#         F.last(col_name, ignorenulls=True).over(windowSpec)
#     )

# bitcoin_data = spark.createDataFrame(bitcoin_data_pd)

# Shape (Number of Rows, Number of Columns)
num_rows = bitcoin_data.count()
num_columns = len(bitcoin_data.columns)

print("Shape: ({}, {})".format(num_rows, num_columns))

# Columns
columns = bitcoin_data.columns
print("Columns: {}".format(columns))

# Check for 'NaN' values
has_nan = bitcoin_data.select([col(c).isNull().alias(c) for c in bitcoin_data.columns]).rdd.flatMap(lambda x: x).collect()
any_nan = any(has_nan)

print("Is There any 'NaN' value: {}".format(any_nan))

# Check for duplicate values
has_duplicates = bitcoin_data.groupBy(bitcoin_data.columns).count().filter("count > 1").count() > 0

print("Is there any duplicate value: {}".format(has_duplicates))

# Feature Engineering
# You can add features like day of the week, hour of the day, etc.
bitcoin_data = bitcoin_data.withColumn("DayOfWeek", dayofweek("Timestamp"))
bitcoin_data = bitcoin_data.withColumn("HourOfDay", hour("Timestamp"))

# Getting more features, last 7 days information to help predict next day closing (can increase more)

# Define the window specification
window_spec = Window().orderBy("Timestamp")


distinct_values = bitcoin_data.select("symbol").distinct().collect()
print(f"distinct_values: {distinct_values}")

indexer = StringIndexer(inputCol="symbol", outputCol="symbol_index")
bitcoin_data = indexer.fit(bitcoin_data).transform(bitcoin_data)

# One-hot encode the indexed column
encoder = OneHotEncoder(inputCol="symbol_index", outputCol="symbol_encoded")
bitcoin_data = encoder.fit(bitcoin_data).transform(bitcoin_data)

lag_cols = ["Open", "High", "Low", "Close", "symbol_encoded", "volume_crypto", "volume_currency"]

print("Old columns:", bitcoin_data.columns)
column_to_drop = 'symbol'
bitcoin_data = bitcoin_data.drop(column_to_drop)
print("New columns:", bitcoin_data.columns)


# Create lagged features for the last 7 days
# for i in lag_cols:
#     for j in range(1,8):
#         bitcoin_data = bitcoin_data.withColumn(col_name + "_b_" + str(j), lag(col_name, j).over(window_spec))

# bitcoin_data = bitcoin_data.dropna() # drop the first rows. They don't have previous information 
print("Updated Data Shape: ", bitcoin_data.count())

# Add next closing value as a label for prediction label
window_spec = Window().orderBy("Timestamp")

bitcoin_data = bitcoin_data.withColumn("upcoming_close", lag('Close', -1).over(window_spec))

# bitcoin_data = bitcoin_data.dropna() # drop the last row. It doesn't have next information 
print("Updated record count after adding prediction close label:", bitcoin_data.count())


#Input all the features in one vector column
feature_columns = [i for i in bitcoin_data.columns if i not in ['upcoming_close', 'Date', 'Timestamp']]
assembler = VectorAssembler(inputCols= feature_columns, outputCol = 'Attributes', handleInvalid="skip")

output = assembler.transform(bitcoin_data) #transform the data using Vector Assembler

#Input vs Output
finalized_data = output.select("Attributes","upcoming_close")

finalized_data.show() # showing the final data

# Feature Selection
feature_columns = [i for i in bitcoin_data.columns if i not in ['upcoming_close', 'Date', 'Timestamp', 'HourOfDay']]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="skip")
# bitcoin_data = assembler.transform(bitcoin_data)

# Train-Test Split - since the problem is time series, we should perform the sequenctial split
prediction_days = int(np.round(bitcoin_data.count()*(10/100),0))
train_data = bitcoin_data.limit(int(bitcoin_data.count()-prediction_days))
test_data = bitcoin_data.exceptAll(train_data)

print("% test data = %", (prediction_days/int(bitcoin_data.count())) * 100)
print("Train data records:", train_data.count())
print("Test data records:", test_data.count())

# Linear Regression Model
lr = LinearRegression(featuresCol="features", labelCol="upcoming_close")

# Pipeline
pipeline = Pipeline(stages=[assembler, lr])

# Model Training
model_lr = pipeline.fit(train_data)

# Make predictions on the test data
predictions = model_lr.transform(test_data)

evaluator_rmse = RegressionEvaluator(labelCol="upcoming_close", predictionCol="prediction", metricName="rmse")
rmse = evaluator_rmse.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse}")

evaluator_r2 = RegressionEvaluator(labelCol="upcoming_close", predictionCol="prediction", metricName="r2")
r2 = evaluator_r2.evaluate(predictions)
print(f"R2: {r2}")

# # View Predictions
# predictions.select("Close", "prediction", *feature_columns).show()