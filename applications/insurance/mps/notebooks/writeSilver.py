# Databricks notebook source
dbutils.widgets.text("Namespace","{*}GenerateResponse/{*}QuotedSchemeList/{*}QuotedScheme","Namespace")
dbutils.widgets.text("OutputPath","/mnt/bronze/steve/mps/silver/QuotedSchemeList")
dbutils.widgets.text("InputTable","mps.bronzestaged")
dbutils.widgets.text("OutputTable","mps.bronzeQuotedSchemeList")

# COMMAND ----------

# MAGIC %run "./mpsFunctions"

# COMMAND ----------

from pyspark.sql.functions import lit, col
from common import common
from common import ingestmetadata
from applications.insurance.mps.library import mps
from applications.insurance.mps.library import constants

def batchProcess(df, epochId):
  dfOut=getObjectListDF(df,spark,dbutils.widgets.get("Namespace"),"")  
  dfOutMeta=ingestmetadata.addCommonMetaData(dfOut)
  dfOutMeta.write.format("delta").mode("append").option("mergeSchema", "true").option("path", dbutils.widgets.get("OutputPath")).saveAsTable(dbutils.widgets.get("OutputTable"))

dfRead=spark.readStream.format("delta").table(dbutils.widgets.get("InputTable"))

dfRead.writeStream.format("delta")\
  .foreachBatch(batchProcess).outputMode("append")\
  .trigger(once=True)\
  .start()
