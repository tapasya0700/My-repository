# Databricks notebook source
# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

from pyspark.sql.functions import *
import datetime

vendorDf=spark.table("bronze.vendtable")
display(vendorDf)





# COMMAND ----------

updatedDateTime=datetime.datetime.now()

dimvendorDf = (vendorDf.select(
vendorDf.VendId.alias("VendorId"),
trim(vendorDf.VendorName).alias("VendorName"),
when(vendorDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(vendorDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
from_utc_timestamp(vendorDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
trim(vendorDf.Address).alias("Address"),
trim(vendorDf.City).alias("City"),
trim(vendorDf.State).alias("State"),
trim(vendorDf.Country).alias("Country"),
trim(vendorDf.ZipCode).alias("ZipCode"),
trim(vendorDf.Region).alias("Region"),
from_utc_timestamp(vendorDf.ValidFrom,'CST').alias("ValidFrom"),
    when(vendorDf.ValidTo.isNull(), "1900-01-01").otherwise(vendorDf.ValidTo).cast("timestamp").alias("ValidTo"),
vendorDf.Active.alias("Active"),
vendorDf.RecordId.alias("VendorRecordId"),
vendorDf.TaxId.alias("TaxId"),
vendorDf.CurrencyCode.alias("CurrencyCode"),

).withColumn("UpdatedDateTime",lit(updatedDateTime))
.withColumn("VendorHashKey", xxhash64("VendorRecordId")) 
.withColumn("VendorDiscount",when(col("Country")=="US",lit("0.01")).when(col("Country")=="UK",lit("0.006")).otherwise("0.0")))                                    




    

display(dimvendorDf)

# COMMAND ----------

dffinal=dimvendorDf
entity="dimvendor"
writetoschema(dffinal,"silver",entity)
