# Databricks notebook source
# MAGIC     %run ./OauthAccess
# MAGIC     

# COMMAND ----------



def fileread(manifestPath,entity):

    
    df2=spark.read.format("com.microsoft.cdm").option("storage","finaloaonstorage.dfs.core.windows.net").option("appid",appid).option("appkey",service_credential).option("tenantid",directoryid).option("manifestPath",f"mycontainmer/oaondirectirectory/Tables/{manifestPath}/{manifestPath}.manifest.cdm.json").option("entity",entity).option("mode", "PERMISSIVE").load()

    return (df2)

# COMMAND ----------

def filewrite(df,deltalakepath):
    df.write.mode("overwrite").option("overwriteSchema","true").option("path",ADLS_PATH+deltalakepath).save()
