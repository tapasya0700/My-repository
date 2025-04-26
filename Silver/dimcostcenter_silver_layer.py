# Databricks notebook source
# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

from pyspark.sql.functions import *
import datetime,dateutil
import pandas as pd


costcenterDf=spark.table("bronze.costcenter")

display(costcenterDf)


# COMMAND ----------


updatedDateTime = datetime.datetime.now()
costcenterfinaldf = (costcenterDf.select(
    costcenterDf.CostCenterNumber,
 when(costcenterDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(costcenterDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
from_utc_timestamp(costcenterDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
costcenterDf.Vat,
costcenterDf.RecordId.alias("costcenterRecordId")
  
    

).withColumn("UpdatedDateTime",lit(updatedDateTime)).withColumn("costcenterHashKey", xxhash64("costcenterRecordId")))
                   


display(costcenterfinaldf)



# COMMAND ----------

dffinal=costcenterfinaldf
entity="dimcostcenter"
writetoschema(dffinal,"silver",entity)
