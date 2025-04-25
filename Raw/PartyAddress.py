# Databricks notebook source
# MAGIC %run ./OauthAccess
# MAGIC

# COMMAND ----------

# MAGIC %run ./filereadandwrite

# COMMAND ----------

manifestPath="Purchase"
entity="PartyAddress"
deltalakepath="DeltaLake/Raw/Purchase/"+entity
df=fileread(manifestPath,entity)
display(df)
filewrite(df,deltalakepath)

# COMMAND ----------


