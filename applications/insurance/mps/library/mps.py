from pyspark.sql.functions import *
from pyspark.sql.types import *

#add ctx later
def getQuotedSchemeList(df,name):
    return df.withColumn("silver", lit(name)

def helloworld():
    return "hello world"