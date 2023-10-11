# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS yas_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS yas_catalog.yas_db

# COMMAND ----------

from pyspark.sql.functions import *
data = [{
"eventTime" : "07-09-2023T15:22:00",
"deviceId": 1,
"value": 1
},
       {
"eventTime" : "07-09-2023T15:27:00",
"deviceId": 1,
"value": 1
},
             {
"eventTime" : "07-09-2023T15:23:00",
"deviceId": 1,
"value": 1
},
                   {
"eventTime" : "07-09-2023T12:15:00",
"deviceId": 1,
"value": 1
} ]

inputDF = spark.createDataFrame(data=data)

inputDF.write.mode("append").json(data_dir)


# COMMAND ----------

data_dir

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/FileStore/Users/yas.mokri@databricks.com/windowing/data

# COMMAND ----------

eventsDF = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .option("cloudFiles.schemaLocation", schema_dir)
        .option("inferColumnTypes", True)
        .load(data_dir)
    )

display(eventsDF)
# windowedCountsDF = \
#   eventsDF \
#     .groupBy(
#       "deviceId",
#       window("eventTime", "1 minutes")) \
#     .count()

# display(windowedCountsDF)

# COMMAND ----------

import random

import uuid
import json

parent_dir = "/FileStore/Users/yas.mokri@databricks.com/windowing"

data_dir = f"{parent_dir}/data"
checkpoint_dir = f"{parent_dir}/checkpoint"
schema_dir = f"{parent_dir}/schema"
delta_dir = f"{parent_dir}/delta"

rescue_dir = f"{parent_dir}/rescue"
checkpoint_rescue_dir = f"{rescue_dir}/checkpoint"
delta_rescue_dir = f"{rescue_dir}/delta_rescue"


# COMMAND ----------

import random
# from faker import Faker
import uuid
import json

parent_dir = "/FileStore/Users/yas.mokri@databricks.com/windowing"

data_dir = f"{parent_dir}/data"
checkpoint_dir = f"{parent_dir}/checkpoint"
schema_dir = f"{parent_dir}/schema"
delta_dir = f"{parent_dir}/delta"

rescue_dir = f"{parent_dir}/rescue"
checkpoint_rescue_dir = f"{rescue_dir}/checkpoint"
delta_rescue_dir = f"{rescue_dir}/delta_rescue"

dbutils.fs.rm(parent_dir, True)
dbutils.fs.mkdirs(data_dir)
dbutils.fs.mkdirs(checkpoint_dir)
dbutils.fs.mkdirs(delta_dir)
dbutils.fs.mkdirs(checkpoint_rescue_dir)
dbutils.fs.mkdirs(delta_rescue_dir)


# fake = Faker()
# Faker.seed(0)


# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table yas_catalog.yas_db.windowing

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from yas_catalog.yas_db.windowing

# COMMAND ----------



# Sample Data is generated for windowing examples
windowingData = (("12", "MP_PP1280_AMPS", "2019-01-02 15:30:00"),
("12", "MP-PP1280_AMPS", "2019-01-02 15:30:30"),
("12", "MP-PP1280_AMPS", "2019-01-02 15:31:00"),
("12", "MP-PP1280_AMPS", "2019-01-02 15:31:50"),
("12", "MP-PP1280_AMPS", "2019-01-02 15:31:55"),
("16", "MP-PP1280_AMPS", "2019-01-02 15:33:00"),
("16", "MP-PP1280_AMPS", "2019-01-02 15:35:20"),
("16", "MP-PP1280_AMPS", "2019-01-02 15:37:00"),
("20", "MP-PP1280_AMPS", "2019-01-02 15:30:30"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 15:31:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 15:31:50"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 15:31:55"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 15:33:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 15:35:20"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 15:37:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 15:40:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 15:45:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 15:46:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 15:47:30"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 15:48:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 15:48:10"),
("20", "MP-PT1285-OUT_ENG", "2019-01-02 15:48:20"),
("20", "MP-PT1285-OUT_ENG", "2019-01-02 15:48:30"),
("20", "MP-PT1285-OUT_ENG", "2019-01-02 15:50:00"),
("20", "MP-PT1285-OUT_ENG", "2019-01-02 15:53:00"),
("20", "MP-PT1285-OUT_ENG", "2019-01-02 15:54:30"),
("20", "MP-PT1285-OUT_ENG", "2019-01-02 15:55:00"),
("22", "MP-PT1285-OUT_ENG", "2019-01-02 15:50:30"),
("22", "MP-PT1285-OUT_ENG", "2019-01-02 15:52:00"),
("22", "MP-PT1285-OUT_ENG", "2019-01-02 15:50:30"),
("22", "MP-PT1285-OUT_ENG", "2019-01-02 15:52:00"),
("22", "MP-PT1285-OUT_ENG", "2019-01-02 15:50:30"),
("22", "MP-PT1285-OUT_ENG", "2019-01-02 15:52:00"))
columns = ["value", "name", "timestamp"]
windowing_df = spark.createDataFrame(data = windowingData, schema = columns)

windowing_df.write.mode("append").json(data_dir)



# COMMAND ----------



# Sample Data is generated for windowing examples
windowingData = (("12", "MP_PP1280_AMPS", "2019-01-02 16:30:00"),
("12", "MP-PP1280_AMPS", "2019-01-02 16:30:30"),
("12", "MP-PP1280_AMPS", "2019-01-02 16:31:00"),
("12", "MP-PP1280_AMPS", "2019-01-02 16:31:50"),
("12", "MP-PP1280_AMPS", "2019-01-02 16:31:55"),
("16", "MP-PP1280_AMPS", "2019-01-02 16:33:00"),
("16", "MP-PP1280_AMPS", "2019-01-02 16:35:20"),
("16", "MP-PP1280_AMPS", "2019-01-02 16:37:00"),
("20", "MP-PP1280_AMPS", "2019-01-02 16:30:30"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 16:31:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 16:31:50"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 16:31:55"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 16:33:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 16:35:20"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 16:37:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 16:40:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 16:45:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 16:46:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 16:47:30"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 16:48:00"),
("20", "MP-LT1275-OUT_ENG", "2019-01-02 16:48:10"),
("20", "MP-PT1285-OUT_ENG", "2019-01-02 16:48:20"),
("20", "MP-PT1285-OUT_ENG", "2019-01-02 16:48:30"),
("20", "MP-PT1285-OUT_ENG", "2019-01-02 16:50:00"),
("20", "MP-PT1285-OUT_ENG", "2019-01-02 16:53:00"),
("20", "MP-PT1285-OUT_ENG", "2019-01-02 16:54:30"),
("20", "MP-PT1285-OUT_ENG", "2019-01-02 16:55:00"),
("22", "MP-PT1285-OUT_ENG", "2019-01-02 16:50:30"),
("22", "MP-PT1285-OUT_ENG", "2019-01-02 16:52:00"),
("22", "MP-PT1285-OUT_ENG", "2019-01-02 16:50:30"),
("22", "MP-PT1285-OUT_ENG", "2019-01-02 16:52:00"),
("22", "MP-PT1285-OUT_ENG", "2019-01-02 16:50:30"),
("22", "MP-PT1285-OUT_ENG", "2019-01-02 16:52:00"))
columns = ["value", "name", "timestamp"]
windowing_df = spark.createDataFrame(data = windowingData, schema = columns)

windowing_df.write.mode("append").json(data_dir)



# COMMAND ----------

from pyspark.sql.functions import *

dbutils.fs.rm(checkpoint_dir, True)
dbutils.fs.mkdirs(checkpoint_dir)

eventsDF = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .option("cloudFiles.schemaLocation", schema_dir)
        .option("inferColumnTypes", True)
        .option("cloudFiles.schemaHints", "timestamp timestamp, name string, value string")
        .load(data_dir)
    )

eventsDF = eventsDF.withColumn("value", col("value").cast("int"))

# display(eventsDF)
tumblingWindows = eventsDF.withWatermark("timestamp", "0 minutes").groupBy("name", window("timestamp", "180 seconds")).agg((max("value")).alias("max"),(min("value")).alias("min"), (max("value")-min("value")).alias("max-min"),collect_list("value").alias("valuelist"), collect_list("timestamp").alias("timelist"))#.withColumn("ingesttime", current_timestamp()) #.count()

windowed_df = tumblingWindows.withColumn("start" , tumblingWindows.window.start).withColumn("ingesttime", current_timestamp()) #.count()

display(windowed_df)
# tumblingWindows.writeStream.outputMode("append").option("checkpointLocation", checkpoint_dir).option("mergeSchema", True).table("yas_catalog.yas_db.windowing")


# COMMAND ----------

windowed_df.schema

# COMMAND ----------

eventsDF.schema

# COMMAND ----------

# MAGIC %sql
# MAGIC select * except (sum) from yas_catalog.yas_db.windowing order by window, name

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yas_catalog.yas_db.silver_windowed_maxmin

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yas_catalog.yas_db.silver_windowed_maxmin

# COMMAND ----------

# Sample Data is generated for windowing examples
windowingData = (("12", "2019-01-02 15:30:00"),
("12",  "2019-01-02 15:57:30"),
("12",  "2019-01-02 15:57:00"),
("12",  "2019-01-02 15:59:50"),
("12",  "2019-01-02 16:00:55"),
("16",  "2019-01-02 16:53:00"),
("16",  "2019-01-02 16:55:20"),
("16",  "2019-01-02 16:59:00"),
("20",  "2019-01-02 16:59:30"),
("20",  "2019-01-02 16:59:00"),
("20",  "2019-01-02 17:11:50"),
("20",  "2019-01-02 17:12:55"),
("20",  "2019-01-02 17:13:00"),
("20",  "2019-01-02 15:35:20"),
("20",  "2019-01-02 15:37:00"),
("20",  "2019-01-02 15:40:00"),
("20",  "2019-01-02 15:45:00"),
("20",  "2019-01-02 15:46:00"),
("20",  "2019-01-02 15:47:30"),
("20",  "2019-01-02 15:48:00"),
("20",  "2019-01-02 15:48:10"),
("20",  "2019-01-02 15:48:20"),
("20",  "2019-01-02 15:48:30"),
("20",  "2019-01-02 15:50:00"),
("20",  "2019-01-02 15:53:00"),
("20",  "2019-01-02 15:54:30"),
("20",  "2019-01-02 15:55:00"),
("22",  "2019-01-02 15:50:30"),
("22",  "2019-01-02 15:52:00"),
("22",  "2019-01-02 15:50:30"),
("22",  "2019-01-02 15:52:00"),
("22",  "2019-01-02 15:50:30"),
("22",  "2020-01-02 15:52:00"))
columns = ["eventId", "timeReceived"]
windowing_df = spark.createDataFrame(data = windowingData, schema = columns)

windowing_df.write.mode("append").json(data_dir)



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yas_catalog.yas_db.windowing order by window desc

# COMMAND ----------

# Sample Data is generated for windowing examples
windowingData = (("12", "2018-01-02 15:30:00"),
("12",  "2018-01-02 15:57:30"),
("12",  "2019-01-02 15:57:00"),
("12",  "2019-01-02 15:59:50"),
("12",  "2019-01-02 16:00:55"),
("16",  "2019-01-02 16:53:00"),
("16",  "2019-01-02 16:55:20"),
("16",  "2019-01-02 16:59:00"),
("20",  "2019-01-02 16:59:30"),
("20",  "2019-01-02 16:59:00"),
("20",  "2019-01-02 17:11:50"),
("20",  "2019-01-02 17:12:55"),
("20",  "2019-01-02 17:13:00"),
("20",  "2019-01-02 15:35:20"),
("20",  "2019-01-02 15:37:00"),
("20",  "2019-01-02 15:40:00"),
("20",  "2019-01-02 15:45:00"),
("20",  "2019-01-02 15:46:00"),
("20",  "2019-01-02 15:47:30"),
("20",  "2019-01-02 15:48:00"),
("20",  "2019-01-02 15:48:10"),
("20",  "2019-01-02 15:48:20"),
("20",  "2019-01-02 15:48:30"),
("20",  "2019-01-02 15:50:00"),
("20",  "2019-01-02 15:53:00"),
("20",  "2019-01-02 15:54:30"),
("20",  "2019-01-02 15:55:00"),
("22",  "2019-01-02 15:50:30"),
("22",  "2019-01-02 15:52:00"),
("22",  "2019-01-02 15:50:30"),
("22",  "2019-01-02 15:52:00"),
("22",  "2020-01-02 15:50:30"),
("22",  "2020-01-02 15:52:00"))
columns = ["eventId", "timeReceived"]
windowing_df = spark.createDataFrame(data = windowingData, schema = columns)

windowing_df.write.mode("append").json(data_dir)



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yas_catalog.yas_db.windowing order by window desc

# COMMAND ----------

# Sample Data is generated for windowing examples
windowingData = (("12", "2018-01-02 15:30:00"),
("12",  "2018-01-02 15:57:30"),
("12",  "2019-01-02 15:57:00"),
("12",  "2019-01-02 15:59:50"),
("12",  "2019-01-02 16:00:55"),
("16",  "2019-01-02 16:53:00"),
("16",  "2019-01-02 16:55:20"),
("16",  "2019-01-02 16:59:00"),
("20",  "2019-01-02 16:59:30"),
("20",  "2019-01-02 16:59:00"),
("20",  "2019-01-02 17:11:50"),
("20",  "2019-01-02 17:12:55"),
("20",  "2019-01-02 17:13:00"),
("20",  "2019-01-02 18:35:20"),
("20",  "2019-01-02 18:37:00"),
("20",  "2019-01-02 18:40:00"),
("20",  "2019-01-02 17:14:00"),
("20",  "2019-01-02 17:15:00"),
("20",  "2019-01-02 17:50:30"),
("20",  "2019-01-02 17:51:00"),
("20",  "2019-01-02 15:48:10"),
("20",  "2019-01-02 15:48:20"),
("20",  "2019-01-02 15:48:30"),
("20",  "2019-01-02 15:50:00"),
("20",  "2019-01-02 15:53:00"),
("20",  "2019-01-02 15:54:30"),
("20",  "2019-01-02 15:55:00"),
("22",  "2019-01-02 15:50:30"),
("22",  "2019-01-02 15:52:00"),
("22",  "2019-01-02 15:50:30"),
("22",  "2019-01-02 15:52:00"),
("22",  "2020-01-02 15:50:30"),
("22",  "2020-01-02 15:52:00"))
columns = ["eventId", "timeReceived"]
windowing_df = spark.createDataFrame(data = windowingData, schema = columns)

windowing_df.write.mode("append").json(data_dir)



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yas_catalog.yas_db.windowing order by window desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(ingesttime) from yas_catalog.yas_db.windowing

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cloud_files_state('dbfs:/FileStore/Users/yas.mokri@databricks.com/windowing/checkpoint')
