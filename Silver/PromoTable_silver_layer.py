# Databricks notebook source
# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

from pyspark.sql.functions import *
import datetime


promoDf=spark.table("bronze.promotable")

display(promoDf)


# COMMAND ----------


updatedDateTime=datetime.datetime.now()

dimpromoDf = promoDf.select(
        promoDf.PromotionId,
        when(promoDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(promoDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        from_utc_timestamp(promoDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        trim(promoDf.PromotionName).alias("PromotionName"),
        promoDf.PromoCode.alias("PromoCode"),
        trim(promoDf.PromoType).alias("PromoType"),
        promoDf.PromoPercentage.alias("PromoPercentage"),
        from_utc_timestamp(promoDf.ValidFrom,"CST").cast("timestamp").alias("ValidFrom"),
        from_utc_timestamp(promoDf.ValidTo,"CST").cast("timestamp").alias("ValidTo"),
        promoDf.IsActive.alias("IsActive"),
        promoDf.RecordId.alias("PromoRecordId")
     
        



         ).withColumn("UpdatedDateTime", lit(updatedDateTime)

    
    ).withColumn("PromoTableHashKey", xxhash64("PromoRecordId")
    )
display(dimpromoDf)

# COMMAND ----------



# COMMAND ----------

dffinal=dimpromoDf
entity="dimpromotable"
writetoschema(dffinal,"silver",entity)
