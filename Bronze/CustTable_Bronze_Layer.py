# Databricks notebook source
# MAGIC %run ../Raw/OauthAccess
# MAGIC

# COMMAND ----------

# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

entity="CustTable"
datapath="DeltaLake/Raw/Sales/"+entity
df=filereadfromdeltalake(datapath,entity)
display(df)
schema="bronze"
writetoschema(df,schema,entity)


# COMMAND ----------


