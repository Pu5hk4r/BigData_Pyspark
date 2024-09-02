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

rawdata = sc.textFile("usdata.csv")

rawdata.foreach(print)

print("========== LENGTH GREATER THAN 200==========")
print()

# LEN

len200 = rawdata.filter( lambda   x  :  len(x) > 200)

len200.foreach(print)


print("========== FLATTEN WITH COMMA==========")
print()


flat = len200.flatMap( lambda  x :  x.split(","))

flat.foreach(print)

print("========== REMOVE HYPHENS==========")
print()


remhy = flat.map(lambda  x : x.replace("-",""))

remhy.foreach(print)


print("========== Concat==========")
print()

concat = remhy.map(lambda   x  : x + ",zeyo")

concat.foreach(print)

sc.stop()