import sys
import os
import urllib
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-22'

from pyspark.sql import SparkSession

# Create SparkSession and configure settings
spark = SparkSession.builder \
    .appName("Read") \
    .master("local[*]") \
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
    .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")  # Set log level

print("---------------------------Raw Data----------------------------------")
print("\n\n\n")

data = sc.textFile("dt.txt")
data.foreach(print)

## ----------------------------------SPLIT DATA---------------------
print("\n\n\n")

mapsplit = data.map(lambda p: p.split(","))

## ----------------------------------DEFINE SCHEMA---------------------

from collections import namedtuple

schema = namedtuple("schema",["txnno", "txndate","amount","category","product","spendby"])

##------------------------------IMPOSE SCHEMA--------------------------

schema_rdd = mapsplit.map(lambda p: schema(p[0],p[1],p[2],p[3],p[4],p[5]))


## -------------------------FILTER SPECIFIC COLUMN---------------------------

print("\n\n\n")

print("-----------------------PRODUCT CONTAIN GYMNASTICS--------------------")

product_filter = schema_rdd.filter(lambda p: p.product and  "Gymnastics" in p.product)
product_filter.foreach(print)


##!!!!!!!!!!!!!!!!!!!!!!!DATA-FRAME-CONVERSION!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


df = product_filter.toDF()

df.show()

#df.write.parquet("file: ///F:  /parquetwrite")

sc.stop()