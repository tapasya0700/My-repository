# Databricks notebook source
# MAGIC %run ./OauthAccess
# MAGIC

# COMMAND ----------

# MAGIC %run ./filereadandwrite

# COMMAND ----------

manifestPath="Sales"
enitity="SalesOrderLine"
deltalakepath="DeltaLake/Raw/Sales/" + enitity
df=fileread(manifestPath,enitity)
display(df)
filewrite(df,deltalakepath)

# COMMAND ----------


