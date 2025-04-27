# Databricks notebook source
# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

from pyspark.sql.functions import *
import datetime,dateutil
import pandas as pd


workerDf=spark.table("bronze.workertable")

display(workerDf)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.dimvertical (
# MAGIC VerticalId BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC  Vertical STRING
# MAGIC )

# COMMAND ----------



UpdatedDateTime = datetime.datetime.now()
Entity = "dimvertical"



workerdf= spark.table("bronze.workertable")



df = workerdf.select(expr("trim(Vertical) AS Vertical")).distinct()
display(df)

# COMMAND ----------




# COMMAND ----------


verticaldf = spark.table("silver.dimvertical")
display(verticaldf)

# COMMAND ----------



# COMMAND ----------

newrowsdf=df.filter(col("Vertical").isNotNull()).exceptAll(verticaldf.select("Vertical"))
display(newrowsdf)



# COMMAND ----------

spark.sql("insert into silver.dimvertical(vertical) select  Vertical from {newrowsdf}",newrowsdf=newrowsdf)

# COMMAND ----------

display(spark.table("silver.dimvertical"))

