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
sc.setLogLevel("ERROR")

print("-----------------------CSV DATAFRAME--------------")

csvdf = spark.read.format("csv").option("header", "true").load("prod.csv")
csvdf.show()

csvdf.createOrReplaceTempView("liya")# assign name to dataframe

procdf= spark.sql("select * from liya where id > 1")

procdf.show()# show used for to see content of dataframe

print("-----------------------END-CSV DATAFRAME--------------")

print("\n\n\n\n\n-----------------------JSON DATAFRAME--------------")

jsondf = spark.read.format("json").load("file4.json")

jsondf.show()

jsondf.createOrReplaceTempView("jdf") # assigned name

procjdf = spark.sql("select * from jdf where state = 'Washington'")


procjdf.show()

print("----------------------- END-JSON DATAFRAME--------------\n\n\n\n\n")

print("-------------------PARQUET DATAFRAME-------------------------")

parquetdf= spark.read.load("file5.parquet") #canbe add here .format("parquet") after read.

parquetdf.show()

parquetdf.createOrReplaceTempView("pdf")

procpdf = spark.sql("select category , cast(sum(amount) as int) as total from pdf group by category")

procpdf.show()

print("-------------------END-PARQUET DATAFRAME-------------------------")