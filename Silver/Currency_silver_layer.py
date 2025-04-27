# Databricks notebook source
# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

from pyspark.sql.functions import *
import datetime

currencyDf=spark.table("bronze.currency")

display(currencyDf)


# COMMAND ----------


updatedDateTime=datetime.datetime.now()

dimcurrencyDf = (currencyDf
    .select(
        currencyDf.CurrencyId.alias("CurrencyId"),
        trim(currencyDf.Code).alias("Code"),
        trim(currencyDf.Country).alias("Country")
        ,currencyDf.RecordId.alias("CurrencyRecordId"),
        trim(currencyDf.CurrencyName).alias("CurrencyName"),
        
        
        
        when(currencyDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(currencyDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        from_utc_timestamp(currencyDf.DataLakeModified_DateTime,'CST').cast("timestamp").alias("DataLakeModified_DateTime"),
       
         ).withColumn("UpdatedDateTime", lit(updatedDateTime)

    
    ).withColumn("CurrencyHashKey", xxhash64("CurrencyRecordId")
    ))
display(dimcurrencyDf)

# COMMAND ----------



# COMMAND ----------

dffinal=dimcurrencyDf
entity="dimCurrency"
writetoschema(dffinal,"silver",entity)
