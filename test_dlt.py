# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

parent_dir = "/FileStore/Users/yas.mokri@databricks.com/windowing"
data_dir = f"{parent_dir}/data"

# COMMAND ----------

# this can be the table that reads from Kafka (Here I'm reading from s3 to simulate kafka)
@dlt.table
def bronze_events():

    eventsDF = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .option("inferColumnTypes", True)
        .option(
            "cloudFiles.schemaHints", "timestamp timestamp, name string, value string"
        )
        .load(data_dir)
    )

    eventsDF = eventsDF.withColumn("value", col("value").cast("int"))

    return eventsDF

# COMMAND ----------

# streaming table which reads new rows from bronze and finds max-min for each name across a 180 sec window
# take note of 
# dlt.read_stream and
# groupBy("name", window("timestamp", "180 seconds"))
# the aggregates agg() are mostly for tracing to check the results
@dlt.table
def silver_windowed_maxmin():

    eventsDF = dlt.read_stream("bronze_events")

    tumblingWindows = (
        eventsDF.withWatermark("timestamp", "0 minutes")
        .groupBy("name", window("timestamp", "180 seconds"))
        .agg(
            (max("value")).alias("max"),
            (min("value")).alias("min"),
            (max("value") - min("value")).alias("max_sub_min"),
            collect_list("value").alias("valuelist"),
            collect_list("timestamp").alias("timelist"),
        )
    )

    windowed_df = tumblingWindows.withColumn(
        "start", tumblingWindows.window.start.cast("timestamp")
    ).withColumn("ingesttime", current_timestamp())

    return windowed_df

# COMMAND ----------

# streaming table which reads new rows from silver_windowed_maxmin and finds the average based on the start of each window used for max-min previously 
# take note that groupBy doean't include name column anymore (as we're intersted in avg across names for a start) 
# .groupBy(window("start", "180 seconds")
@dlt.table
def silver_windowed_avg():

    eventsDF = dlt.read_stream("silver_windowed_maxmin")

    tumblingWindows = (
        eventsDF.withWatermark("start", "0 minutes")
        .groupBy(window("start", "180 seconds"))
        .agg(
            (avg("max_sub_min")).alias("avg"),
            collect_list("max_sub_min").alias("max_sub_minlist"),
            collect_list("start").alias("startlist"),
            collect_list("name").alias("namelist"),
        )
    )

    windowed_df = tumblingWindows.withColumn(
        "start", tumblingWindows.window.start.cast("timestamp")
    ).withColumn(
        "ingesttime", current_timestamp()
    ) 

    return windowed_df


