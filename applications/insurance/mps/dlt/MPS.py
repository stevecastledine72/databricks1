# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

#@dlt.expect
#@dlt.expect_or_fail
#@dlt.expect_or_drop
#@dlt.expect_all
#@dlt.expect_all_or_drop
#@dlt.expect_all_or_fail

# COMMAND ----------

import dlt
@dlt.table(
  comment="MPS Silver QuotedSchemeList",
  path="/mnt/bronze/steve/mps/silver/QuotedSchemeList"
)
#####################################################################
#We can host these rules centrally and bring them (so analyst can control in a spreadsheet or other)
#####################################################################

@dlt.expect("BasicPremium_Amount null", "BasicPremium_Amount IS NOT NULL")
@dlt.expect("CarTerms null", "CarTerms IS NOT NULL")
@dlt.expect("ImpliedCostPNCD_Amount", "ImpliedCostPNCD_Amount IS NOT NULL")
@dlt.expect("IsDrivingDiscount null", "IsDrivingDiscount IS NOT NULL")
@dlt.expect("BasicPremium_Amount valid premium", "BasicPremium_Amount > 200")
def SILVER_QuotedSchemeList():
  return spark.readStream.format("delta").table("mps.bronze_quotedschemelist") \
    .withColumn("BasicPremium_Amount", col("BasicPremium_Amount").cast(IntegerType())) \
    .withColumn("PolicyStartDate", to_date(col("PolicyStartDate")).cast(TimestampType())) \

@dlt.table(
  comment="MPS Gold QuotedSchemeList - Distinct InsurerName",
  path="/mnt/bronze/steve/mps/gold/QuotedSchemeListInsurerName"
)
def GOLD_QuotedSchemeListInsurerName():
  return (
    dlt.read('SILVER_QuotedSchemeList')
         .groupBy('InsurerName')
            .agg(
            count("InsurerName").alias("count")
            )
         .orderBy('InsurerName')
  )

@dlt.table(
  comment="MPS Gold QuotedSchemeList - Distinct Policy Start Date",
  path="/mnt/bronze/steve/mps/gold/QuotedSchemeListDistinct"
)
def GOLD_QuotedSchemeListDistinct():
  return (
    dlt.read('SILVER_QuotedSchemeList')
         .groupBy('PolicyStartDate')
            .agg(
            count("PolicyStartDate").alias("count")
            )
         .orderBy('PolicyStartDate')
  )
