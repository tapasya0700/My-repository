# Databricks notebook source
# MAGIC %run ./OauthAccess
# MAGIC

# COMMAND ----------

# MAGIC %run ./filereadandwrite

# COMMAND ----------

manifestPath="Sales"
enitity="PromoTable"
deltalakepath="DeltaLake/Raw/Sales/" + enitity
df=fileread(manifestPath,enitity)
display(df)
filewrite(df,deltalakepath)

# COMMAND ----------


