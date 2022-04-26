# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys, os

#sys.path.append(os.path.abspath('/Workspace/Repos/steve.castledine1@theaa.com/databricks1'))
from common import common
from applications.insurance.mps.library import mps

print (common.commonfunction("Steve"))

# COMMAND ----------

from pyspark.sql.functions import lit
from common import common
from applications.insurance.mps.library import mps
from applications.insurance.mps.library import constants

df=mps.getObjectListDF(spark.table("mps.bronze"), spark, lit(constants.CONST_XPATH_QUOTEDSCHEME), "")

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys, os
import xml.etree.ElementTree as ET
import re, json

#UDF Function to pull out model from XML
def getXMLFlatModel(xmlAsString, CorrelationId, CustomerQuoteReference, QuoteType, parentNode):
  #try:
    QuotedSchemeListArray = []
    
    xml = ET.fromstring(xmlAsString)
  
    for element in xml.findall(".//"+parentNode):
       
      #keep this as a reminder
      #qi=element.find(".//{*}MerlinBreakdownString")
      #QuotedSchemeListArray.append((qi.text if qi is not None else "","steve","paul","ian")) 
      
      itemArray=[]
      jsonArray=[]

      parenttag=""
    
      #Add metadata first (comprises of keys etc)
      jsonArray.append({"CorrelationId":CorrelationId})
      jsonArray.append({"CustomerQuoteReference":CustomerQuoteReference})
      jsonArray.append({"QuoteType":QuoteType})
      
      for child in element.getchildren():
        #Create a non namespace tag string (ie remove the crappy namespace)
        tag=re.sub(r'{[^()]*}', '', child.tag)
        
        #Delete lists as we will retrieve this seperately
        if tag.endswith("List"):
          element.remove(child)
          break
   
        if child.text is None:
          # this is where we need to find stuff like the amount/currency pairs
          parenttag=tag
          for item in child.iter():
            tag=re.sub(r'{[^()]*}', '', item.tag)
            if (tag=="Amount" or tag=="Currency") and item.text is not None:
              jsonArray.append({parenttag+":"+tag:item.text})
        else:
          # this is an data element of the parent so use
          jsonArray.append({tag:child.text})
      
      #Convert to a usable json string
      itemArray.append((json.dumps(jsonArray).replace("{","").replace("}","").replace("[","{").replace("]","}")))
      
      #Add this row to array
      QuotedSchemeListArray.append([itemArray])

    return QuotedSchemeListArray
  
  #except:
    #return []
    
#Declare UDF
schema = ArrayType(StructType([
    StructField("name", ArrayType(StringType()), True)
]))
extractXMLFlatModel = udf(getXMLFlatModel, schema)

#Get bronze table
dfMPSBronze=spark.table("mps.bronze")
display(dfMPSBronze.select("body").limit(1))
#This data frame takes the bronze data, breaks out the model we want and creates a seperate row per iteration of that model in the XML Document in json format
#Declare model we want to retrieve in XML XPath format
model=lit("{*}GenerateResponse/{*}FailedSchemeList/{*}FailedScheme")
dfModel=dfMPSBronze.withColumn("ModelList", explode(extractXMLFlatModel(col("Body"), col("CorrelationId"), col("CustomerQuoteReference"), col("type"), model)))
display(dfModel)
dfModel=dfModel.select(col("ModelList.name")[0].alias("json"))

display(dfModel)

#Now we have the json representation we can create a new Dataframe automatically infering schema and therefore columns
dfMPSSilver = spark.read.json(dfModel.rdd.map(lambda r: r.json))
display(dfMPSSilver)

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
