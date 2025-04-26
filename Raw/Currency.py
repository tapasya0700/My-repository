# Databricks notebook source
# MAGIC %run ./OauthAccess
# MAGIC

# COMMAND ----------

# MAGIC %run ./filereadandwrite

# COMMAND ----------

manifestPath="Others"
enitity="Currency"
deltalakepath="DeltaLake/Raw/Others/" + enitity
df=fileread(manifestPath,enitity)
display(df)
filewrite(df,deltalakepath)

# COMMAND ----------


