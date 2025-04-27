# Databricks notebook source
# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

from pyspark.sql.functions import *
import datetime


paymentDf=spark.table("bronze.paymenttypes")

display(paymentDf)


# COMMAND ----------

salesorderdf=spark.table("bronze.salesorderline")
display(salesorderdf)


# COMMAND ----------

salesdf=salesorderdf.select(salesorderdf.PaymentTypeDesc).distinct()
display(salesdf)


# COMMAND ----------



newrowsdf=salesdf.exceptAll(paymentDf.select("PaymentTypeName"))
display(newrowsdf)



# COMMAND ----------


# COMMAND ----------

maxdf = spark.sql("select ifnull(max(PaymentTypeId),0) as maxid from {df}",df=paymentDf)
toprow = maxdf.head(1)
maxid = toprow[0][0]
print(maxid)

# COMMAND ----------


# COMMAND ----------


import pyspark.sql.window as W

# COMMAND ----------

idsdf = newrowsdf.withColumn("PaymentTypeId", row_number().over(window=W.Window.orderBy(col("PaymentTypeDesc"))))
display(idsdf)

# COMMAND ----------



# COMMAND ----------

idsFinal = idsdf.withColumn("PaymentTypeId", (col("PaymentTypeId")+maxid).cast("long"))
display(idsFinal)

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from silver.dimpaymenttypes

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC



# COMMAND ----------


# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------


# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from silver.dimpaymenttypes

# COMMAND ----------


# COMMAND ----------



# COMMAND ----------

paymentDf=spark.table("bronze.paymenttypes")

display(paymentDf)

# COMMAND ----------

# Only selecting PaymentTypeId and PaymentTypeName from first dataframe
df1_selected = paymentDf.select("PaymentTypeId", "PaymentTypeName")

# Make sure idsFinal also has PaymentTypeId and PaymentTypeName
# (if idsFinal has only PaymentTypeId and PaymentTypeDesc, you might need to rename PaymentTypeDesc to PaymentTypeName)

# If needed, rename column in idsFinal
idsFinal_renamed = idsFinal.withColumnRenamed("PaymentTypeDesc", "PaymentTypeName")

# Now union
final_df = idsFinal_renamed.unionByName(df1_selected)
final_df1=final_df.withColumnRenamed("PaymentTypeName","PaymentTypeDesc")
# Display
display(final_df1)


# COMMAND ----------

Entity = "dimpaymenttypes"
writetoschema(final_df1,"silver",Entity)
