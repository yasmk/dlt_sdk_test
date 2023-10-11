# Databricks notebook source
parent_dir = "/FileStore/Users/yas.mokri@databricks.com/windowing"
data_dir = f"{parent_dir}/data"

dbutils.fs.rm(parent_dir, True)
dbutils.fs.mkdirs(data_dir)


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


