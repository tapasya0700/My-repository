# Databricks notebook source
# MAGIC %run ./OauthAccess
# MAGIC

# COMMAND ----------

# MAGIC %run ./filereadandwrite

# COMMAND ----------

manifestPath="Hr"
enitity="WorkerTable"
deltalakepath="DeltaLake/Raw/Hr/" + enitity
df=fileread(manifestPath,enitity)
display(df)
filewrite(df,deltalakepath)

# COMMAND ----------


