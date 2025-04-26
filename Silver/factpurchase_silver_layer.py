# Databricks notebook source
# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

from pyspark.sql.functions import *
import datetime,dateutil
import pandas as pd


purchaseorderDf=spark.table("bronze.purchaseorder")

display(purchaseorderDf)


# COMMAND ----------


costcenterDf=spark.table("silver.dimcostcenter")

display(costcenterDf)






# COMMAND ----------

currencyDf=spark.table("silver.dimcurrency")

display(currencyDf)



# COMMAND ----------


updatedDateTime = datetime.datetime.now()
finaldf=(purchaseorderDf.join(costcenterDf, purchaseorderDf.CostCenter == costcenterDf.CostCenterNumber.cast("string"), "left").join(currencyDf, purchaseorderDf.currencycode == currencyDf.Code, "left")
         
         
         .select(

    purchaseorderDf.PoNumber.alias("PoNumber"),
    purchaseorderDf.LineItem.alias("LineItem"),
    purchaseorderDf.VendId.alias("VendorKey"  ),
    when(purchaseorderDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(purchaseorderDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
    from_utc_timestamp(purchaseorderDf.DataLakeModified_DateTime,"CST").cast("timestamp").alias("DataLakeModified_DateTime"),
    purchaseorderDf.Qty.alias("Qty"),
    purchaseorderDf.PurchasePrice.alias("PurchasePrice"),
    purchaseorderDf.TotalOrder.alias("TotalOrder"),
    purchaseorderDf.CostCenter.alias("CostCenterKey"),
    costcenterDf.Vat.alias("VatAmount"),
    round(purchaseorderDf.TotalOrder+(purchaseorderDf.TotalOrder*costcenterDf.Vat),4).alias("TotalAmount"),

    purchaseorderDf.ExchangeRate.alias("ExchangeRate"),
    
    purchaseorderDf.Itemkey.alias("Itemkey"),
    currencyDf.CurrencyId.alias("CurrencyKey"),
    
    from_utc_timestamp(purchaseorderDf.OrderDate,"CST").cast("timestamp").alias("OrderDate"),
    from_utc_timestamp(purchaseorderDf.ShipDate,"CST").cast("timestamp").alias("ShipDate"),
    from_utc_timestamp(purchaseorderDf.DeliveredDate,"CST").cast("timestamp").alias("DeliveredDate"),

    date_format(purchaseorderDf.OrderDate,"yyyymmdd").cast("int").alias("OrderDateKey"),
    date_format(purchaseorderDf.ShipDate,'yyyyMMdd').cast("int").alias("ShipDateKey"),
    date_format(purchaseorderDf.DeliveredDate,'yyyyMMdd').cast("int").alias("DeliveredDateKey"),
    purchaseorderDf.TrackingNumber.alias("TrackingNumber"),
    purchaseorderDf.Batchid.alias("BatchId"),
    purchaseorderDf.CreatedBy.alias("CreatedBy"),
    purchaseorderDf.RecordId.alias("PurchaseOrderRecordId"),
    purchaseorderDf.CategoryId.alias("CategoryKey")
    
   


).withColumn("UpdatedDateTime",lit(updatedDateTime))
.withColumn("PurchaseOrderHashKey",xxhash64("PurchaseOrderRecordId"))
         )
display(finaldf)


# COMMAND ----------

dffinal=finaldf
entity="factpurchaseorder"
writetoschema(dffinal,"silver",entity)
