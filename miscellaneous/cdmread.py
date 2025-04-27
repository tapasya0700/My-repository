# Databricks notebook source
service_credential = dbutils.secrets.get(scope="oaonscope",key="clientsecret")

spark.conf.set("fs.azure.account.auth.type.finaloaonstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.finaloaonstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.finaloaonstorage.dfs.core.windows.net", dbutils.secrets.get(scope="oaonscope",key="appid"))
spark.conf.set("fs.azure.account.oauth2.client.secret.finaloaonstorage.dfs.core.windows.net", service_credential)

directoryid=dbutils.secrets.get(scope="oaonscope",key="tenantid")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.finaloaonstorage.dfs.core.windows.net", f"https://login.microsoftonline.com/{directoryid}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://mycontainmer@finaloaonstorage.dfs.core.windows.net/oaondirectirectory/Tables/Purchase"))





# COMMAND ----------


df=spark.read.format("csv").option("inferSchema","true").option("path","abfss://mycontainmer@finaloaonstorage.dfs.core.windows.net/oaondirectirectory/Tables/Purchase/Parties_001.csv").load()

display(df)


# COMMAND ----------


service_credential = dbutils.secrets.get(scope="oaonscope",key="clientsecret")
directoryid=dbutils.secrets.get(scope="oaonscope",key="tenantid")
appid=dbutils.secrets.get(scope="oaonscope",key="appid")
df2=spark.read.format("com.microsoft.cdm").option("storage","finaloaonstorage.dfs.core.windows.net").option("appid",appid).option("appkey",service_credential).option("tenantid",directoryid).option("manifestPath","mycontainmer/oaondirectirectory/Tables/Purchase/Purchase.manifest.cdm.json").option("entity","Parties").option("mode", "PERMISSIVE").load()

display(df2)

# COMMAND ----------


