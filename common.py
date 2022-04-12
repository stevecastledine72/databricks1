from pyspark.sql.functions import *
from pyspark.sql.types import *

def commonfunction(name):
  return "hello "+name

def newfunction(name):
    return name

def processDF(df):
    return df.withColumn("silver", lit("Hello Silver World")).withColumn("repos", lit("Hello repos"))