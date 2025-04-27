# Databricks notebook source
# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

from pyspark.sql.functions import *
import datetime


custDf=spark.table("bronze.custtable")

display(custDf)


# COMMAND ----------


updatedDateTime=datetime.datetime.now()

dimcustDf = custDf.select(
        custDf.CustomerId,
        trim(custDf.CustomerName).alias("CustomerName"),
        trim(custDf.Email).alias("Email"),
        trim(custDf.Phone).alias("Phone"),
        trim(custDf.Address).alias("Address"),
        trim(custDf.City).alias("City"),
        trim(custDf.State).alias("State"),
        trim(custDf.Country).alias("Country"),
        trim(custDf.ZipCode).alias("ZipCode"),
        trim(custDf.Region).alias("Region"),
        from_utc_timestamp(custDf.SignupDate,"CST").cast("timestamp").alias("SignupDate"),
        
        custDf.RecordId.alias("CustRecordId"),
       
        when(custDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(custDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        from_utc_timestamp(custDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),



         ).withColumn("UpdatedDateTime", lit(updatedDateTime)

    
    ).withColumn("CustTableHashKey", xxhash64("CustRecordId")
    )
display(dimcustDf)

# COMMAND ----------



# COMMAND ----------

dffinal=dimcustDf
entity="dimcustomer"
writetoschema(dffinal,"silver",entity)
