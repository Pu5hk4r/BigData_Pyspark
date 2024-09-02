import sys
import os
import urllib
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-22'

from pyspark import SparkConf

from pyspark import  SparkContext

from pyspark.sql import SparkSession

conf = SparkConf()

conf.set("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation")   # pushkar added
conf.set("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation")    # pushkar added



conf = SparkConf().setAppName("Read").setMaster("local[*]")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")  #Pushkar added extra ERROR,WARN,INFO ,DEBUG


spark = SparkSession.builder.getOrCreate()

print()
print("======= RAW DATA=======")
print()

rawdata = sc.textFile("dt.txt")

rawdata.foreach(print)

print()
print("======= Rows Contains Gymnastics=======")
print()

fildata = rawdata.filter(lambda x : 'Gymnastics' in x )
fildata.foreach(print)

sc.stop()