# Databricks notebook source
# MAGIC %run ../Raw/OauthAccess
# MAGIC

# COMMAND ----------

# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

entity="FiscalPeriod"
datapath="DeltaLake/Raw/Others/"+entity
df=filereadfromdeltalake(datapath,entity)
display(df)
schema="bronze"
writetoschema(df,schema,entity)


# COMMAND ----------


