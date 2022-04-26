# Databricks notebook source
from common import common
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json, os, re, sys
import xml.etree.ElementTree as ET

#sys.path.append(os.path.abspath('/Workspace/Repos/steve.castledine1@theaa.com/databricks1'))

#from common import common

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
        
        #Delete lists as we will retrieve these seperately
        if tag.endswith("List"):
          element.remove(child)
          break
   
        if child.text is None:
          # this is where we need to find stuff like the amount/currency pairs
          parenttag=tag
          for item in child.iter():
            tag=re.sub(r'{[^()]*}', '', item.tag)
            if (tag=="Amount" or tag=="Currency") and item.text is not None:
              jsonArray.append({parenttag+"_"+tag:item.text})
        else:
          # this is an data element of the parent so use
          jsonArray.append({tag:child.text})
      
      #Convert to a usable json string (because I probably didnt need the {} in the first place so revisit)
      itemArray.append((json.dumps(jsonArray).replace("{","").replace("}","").replace("[","{").replace("]","}")))
      
      #Add this row to array
      QuotedSchemeListArray.append([itemArray])

    return QuotedSchemeListArray
  
  #except:
    #return []
    
#Declare UDF
#will schema work without field name?
schema = ArrayType(StructType([
    StructField("name", ArrayType(StringType()), True)
]))
extractXMLFlatModel = udf(getXMLFlatModel, schema)

##############################################################################################################
#################Callable Functions
def getObjectListDF(df,xxspark,parentNode,ctx):
  
  #This data frame takes the bronze data, breaks out the model we want and creates a seperate row per iteration of that model in the XML Document in json format
  #Declare model we want to retrieve in XML XPath format
  model=lit(parentNode)  
  dfModel=df.withColumn("ModelList", explode(extractXMLFlatModel(col("Body"), col("CorrelationId"), col("CustomerQuoteReference"), col("type"), model))).select(col("ModelList.name")[0].alias("json"))
 
  #Now we have the json representation we can create a new Dataframe automatically infering schema and therefore columns
  dfMPSSilver = spark.read.json(dfModel.rdd.map(lambda r: r.json))
  
  return dfMPSSilver
