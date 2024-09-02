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

csvdf = spark.read.format("csv").load("dt.txt").toDF("txnno","txndate","amount","category","product","spendby") # to assign columns toDF used

print("---------------------CSV DATA-------------------------------------\n\n")
csvdf.show()

#csvdf.createOrReplaceTempView("cdf")

print("\n\n\n------------------SHOWING TWO COLUMNS -CSV-------------------")

procdf = csvdf.select("txndate","product")

procdf.show()

print("\n\n\n------------------DROPPED TWO COLUMNS -CSV-------------------")

procdf = csvdf.drop("txndate","product")

procdf.show()