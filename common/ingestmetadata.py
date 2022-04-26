from pyspark.sql.functions import *

def addCommonMetaData(df):
  returndf= df.withColumn("META_TODAY",current_date()) \
    .withColumn("META_TAG",lit("Insurance"))
    
  return returndf