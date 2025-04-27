# Databricks notebook source
# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

from pyspark.sql.functions import *
import datetime,dateutil
import pandas as pd


workerDf=spark.table("bronze.workertable")
verticalDf=spark.table("silver.dimvertical")




# COMMAND ----------


updatedDateTime = datetime.datetime.now()
joindf=(workerDf.join(verticalDf,workerDf.Vertical==verticalDf.Vertical,"left").select(



    workerDf.WorkerID.alias("WorkerID"),
   when(workerDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(workerDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
from_utc_timestamp(workerDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
    workerDf.SupervisorId.alias("SupervisorId"),
    trim(workerDf.WorkerName).alias("WorkerName"),
    trim(workerDf.WorkerEmail).alias("WorkerEmail"),
    trim(workerDf.Phone).alias("Phone"),
    from_utc_timestamp(workerDf.DOJ,"CST").cast("timestamp").alias("DOJ"),
    from_utc_timestamp(workerDf.DOL,"CST").cast("timestamp").alias("DOL"),
    
    trim(workerDf.Type).alias("Type"),
    workerDf.PayPerAnnum.alias("PayPerAnnum"),
    workerDf.Rate.alias("Rate"),
    workerDf.RecordId.alias("WorkerRecordId"),
    verticalDf.VerticalId.alias("VerticalId")
).withColumn("UpdatedDateTime",lit(updatedDateTime)).withColumn("WorkerHashKey", xxhash64("WorkerRecordId")))
display(joindf)


# COMMAND ----------

dffinal=joindf
entity="dimworkertable"
writetoschema(dffinal,"silver",entity)
