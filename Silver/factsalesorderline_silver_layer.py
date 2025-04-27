# Databricks notebook source
# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

from pyspark.sql.functions import *
import datetime,dateutil
import pandas as pd


salesorderlineDf=spark.table("bronze.salesorderline")
dimcurrencyDf=spark.table("silver.dimcurrency")
dimpaymenttermDf=spark.table("silver.dimpaymenttypes")
dimpromotableDf=spark.table("silver.dimpromotable")
salesorderlineDf = salesorderlineDf.drop("TotalOrder")






# COMMAND ----------

display(salesorderlineDf)

# COMMAND ----------

dimpaymenttermDf=spark.table("silver.dimpaymenttypes")
display(dimpaymenttermDf)

# COMMAND ----------

promotable_processed_df = dimpromotableDf.select(
    "PromotionId",
    when(col("PromotionName") == "Volume Discount 11 to 20", 11)
    .when(col("PromotionName") == "Volume Discount 21 to 40", 21)
    .when(col("PromotionName") == "Volume Discount 41 to 60", 41)
    .when(col("PromotionName") == "Volume Discount > 60", 61)
    .otherwise(None).alias("VolumeStart"),
    
    when(col("PromotionName") == "Volume Discount 11 to 20", 20)
    .when(col("PromotionName") == "Volume Discount 21 to 40", 40)
    .when(col("PromotionName") == "Volume Discount 41 to 60", 60)
    .when(col("PromotionName") == "Volume Discount > 60", 9999999)
    .otherwise(None).alias("VolumeEnd"),

    "ValidFrom",
    "ValidTo",
    "PromoPercentage"
)
display(promotable_processed_df)

# COMMAND ----------

from pyspark.sql.types import DecimalType

decimal_type = DecimalType(18, 4)
updatedDateTime = datetime.datetime.now()

df_final = (
    salesorderlineDf.join(dimcurrencyDf, salesorderlineDf.CurrencyCode == dimcurrencyDf.Code, "left")
    .join(dimpaymenttermDf, salesorderlineDf.PaymentTypeDesc == dimpaymenttermDf.PaymentTypeDesc, "left")
    .join(promotable_processed_df,
          (
              (month(salesorderlineDf.BookDate) == 1) &
              (salesorderlineDf.BookDate.between(promotable_processed_df.ValidFrom, promotable_processed_df.ValidTo))
          ) |
          (
              (month(salesorderlineDf.BookDate) != 1) &
              (salesorderlineDf.Qty.between(promotable_processed_df.VolumeStart, promotable_processed_df.VolumeEnd))
          ), "left")
    .select(
        salesorderlineDf.SalesOrderNumber,
        salesorderlineDf.SalesOrderLine,
        when(salesorderlineDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(salesorderlineDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        from_utc_timestamp(salesorderlineDf.DataLakeModified_DateTime, "CST").cast("timestamp").alias("DataLakeModified_DateTime"),

        salesorderlineDf.ItemId,
        salesorderlineDf.Qty.alias("Qty"), 
        salesorderlineDf.Price.alias("Price"),  # Added casting
        (salesorderlineDf.Qty * salesorderlineDf.Price).cast(decimal_type).alias("TotalOrder"),
        (salesorderlineDf.VatPercentage).cast(decimal_type).alias("VatPercentage"),  # Added casting

        when(
            promotable_processed_df.PromoPercentage.isNull(),
            salesorderlineDf.Qty * salesorderlineDf.Price
        ).otherwise(
            salesorderlineDf.Qty * salesorderlineDf.Price * (1 - promotable_processed_df.PromoPercentage / 100)
        ).cast(decimal_type).alias("TotalAmountWithDiscount"),

        (when(
            promotable_processed_df.PromoPercentage.isNull(),
            salesorderlineDf.Qty * salesorderlineDf.Price
        ).otherwise(
            salesorderlineDf.Qty * salesorderlineDf.Price * (1 - promotable_processed_df.PromoPercentage / 100)
        ) * salesorderlineDf.VatPercentage).cast(decimal_type).alias("VatAmount"),

        (
            (
                when(
                    promotable_processed_df.PromoPercentage.isNull(),
                    salesorderlineDf.Qty * salesorderlineDf.Price
                ).otherwise(
                    salesorderlineDf.Qty * salesorderlineDf.Price * (1 - promotable_processed_df.PromoPercentage / 100)
                )
            ) +
            (
                when(
                    promotable_processed_df.PromoPercentage.isNull(),
                    salesorderlineDf.Qty * salesorderlineDf.Price
                ).otherwise(
                    salesorderlineDf.Qty * salesorderlineDf.Price * (1 - promotable_processed_df.PromoPercentage / 100)
                )
            )
        ).cast(decimal_type).alias("TotalAmount"),

        dimcurrencyDf.CurrencyId.alias("CurrencyId"),

        from_utc_timestamp(salesorderlineDf.BookDate, "CST").cast("timestamp").alias("BookDate"),
        date_format(salesorderlineDf.BookDate, "yyyyMMdd").cast("int").alias("BookDateKey"),
        from_utc_timestamp(salesorderlineDf.ShippedDate, "CST").cast("timestamp").alias("ShipDate"),
        date_format(salesorderlineDf.ShippedDate, "yyyyMMdd").cast("int").alias("ShipDateKey"),
        from_utc_timestamp(salesorderlineDf.DeliveredDate, "CST").cast("timestamp").alias("DeliveredDate"),
        date_format(salesorderlineDf.DeliveredDate, "yyyyMMdd").cast("int").alias("DeliveredKey"),

        salesorderlineDf.TrackingNumber.alias("TrackingNumber"),
        salesorderlineDf.CustId.alias("CustId"),
        dimpaymenttermDf.PaymentTypeId.alias("PaymentTypeId"),

        dimpromotableDf.PromotionId.alias("PromotionId"),
        salesorderlineDf.RecordId.alias("SalesOrderLineRecordId"),
    )
    .withColumn("UpdatedDateTime", lit(updatedDateTime))
    .withColumn("SalesOrderLineHashKey", xxhash64("SalesOrderLineRecordId"))
)

display(df_final)


# COMMAND ----------

dffinal=df_final
entity="factsalesorderline"
writetoschema(dffinal,"silver",entity)
