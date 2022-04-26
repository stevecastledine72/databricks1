-- Databricks notebook source
DROP TABLE IF EXISTS mps.bronze_staged;
DROP TABLE IF EXISTS mps.bronze_quotedschemelist;
DROP TABLE IF EXISTS mps.bronze_referredschemelist;
DROP TABLE IF EXISTS mps.silver_quotedschemelist;
DROP TABLE IF EXISTS mps.gold_quotedschemelistdistinct;
DROP TABLE IF EXISTS mps.gold_quotedschemelistinsurername;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/mnt/bronze/steve/mps/

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS MPS COMMENT 'This is an MPS schema' LOCATION "/mnt/bronze/steve/mps";
CREATE TABLE IF NOT EXISTS mps.bronze_staged (body STRING, CorrelationId STRING, CustomerQuoteReference STRING, type STRING) LOCATION "/mnt/bronze/steve/mps/bronze/staged";

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/pipelines/dbf90e5c-fdb8-4d3f-aa83-c70975b8f459
