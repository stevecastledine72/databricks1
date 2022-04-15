# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys, os

sys.path.append(os.path.abspath('/Workspace/Repos/steve.castledine1@theaa.com/databricks1'))
from common import common
from applications.insurance.mps.library import mps

#print (common.commonfunction("Steve"))

print (mps.helloworld())

# COMMAND ----------


