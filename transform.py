# Databricks notebook source
display(dbutils.fs.ls("/mnt/Raw/vehiclesdata/")) 

# COMMAND ----------

dbutils.fs.ls('mnt/Raw/vehiclestransform')

# COMMAND ----------

input_path = '/mnt/Raw/vehiclesdata/vehiclesdata.csv'

# COMMAND ----------

df = spark.read.format("csv").load(input_path)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

rdd = sc.textFile('mnt/Raw/vehiclesdata/vehiclesdata.csv').zipWithIndex().filter(lambda a:a[1]>3).map(lambda a:a[0].split(","))
rdd.collect()

# COMMAND ----------

column_rdd = rdd.first()

column_rdd

# COMMAND ----------

main_rdd = rdd.filter(lambda a:a!=column_rdd)
Vehicle_crash= main_rdd.toDF(column_rdd)

# COMMAND ----------

display(Vehicle_crash)

# COMMAND ----------

Vehicle_crash.write.mode("overwrite").option("header", "true").csv("/mnt/Raw/vehiclestransform/Vehiclescrash")

# COMMAND ----------

Vehicle_crash.repartition(4).write.mode("overwrite").option("header", "true").csv("/mnt/Raw/vehiclestransform/Vehiclescrash")

# COMMAND ----------

from pyspark.sql.functions import *

Vehicle_crash.groupBy("Road_Type").agg(count('*').alias("Accident")).show()

# COMMAND ----------

df1 = Vehicle_crash[Vehicle_crash['Accident_Index'] != 'BS0000001']
display(df1)
