# Databricks notebook source
service_credential = dbutils.secrets.get(scope="oaonscope",key="clientsecret")

spark.conf.set("fs.azure.account.auth.type.finaloaonstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.finaloaonstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.finaloaonstorage.dfs.core.windows.net", dbutils.secrets.get(scope="oaonscope",key="appid"))
spark.conf.set("fs.azure.account.oauth2.client.secret.finaloaonstorage.dfs.core.windows.net", service_credential)

directoryid=dbutils.secrets.get(scope="oaonscope",key="tenantid")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.finaloaonstorage.dfs.core.windows.net", f"https://login.microsoftonline.com/{directoryid}/oauth2/token")


service_credential = dbutils.secrets.get(scope="oaonscope",key="clientsecret")
directoryid=dbutils.secrets.get(scope="oaonscope",key="tenantid")
appid=dbutils.secrets.get(scope="oaonscope",key="appid")

ADLS_PATH="abfss://mycontainmer@finaloaonstorage.dfs.core.windows.net/"
