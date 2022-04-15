# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys, os

sys.path.append(os.path.abspath('/Workspace/Repos/steve.castledine1@theaa.com/databricks1'))
from common import common
from applications.insurance.mps.library import mps

@dlt.table(
  comment="Bronze Table"
)
def streaming_bronze():
  return spark.readStream.format("delta").table("basicstream.bronze")

@dlt.table(
  comment="Silver Table"
)
def streaming_silver():
  return mps.getQuotedSchemeList(dlt.read_stream("streaming_bronze"), common.commonfunction("Steve"))
