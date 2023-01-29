# Databricks notebook source
storage_account_name = "dcdatalakestorage"
client_id = "34da25bc-a3d8-4bc2-a8b4-4ad5a0b9c273"
tenant_id = "825a6c46-0494-4618-918a-0441370f6939"
client_secret = "Pee8Q~sp7h3ejkCWmoz7IhqEXYf1dLpsHQu_~ba3"

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
		   
		   
def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)


mount_adls("app")



%fs ls

# COMMAND ----------

dbutils.fs.ls("/mnt/dcdatalakestorage/app/divesh")

df=spark.read.csv("dbfs:/mnt/dcdatalakestorage/app/divesh/New Delhi Branch.csv",header=True)

display(df)
