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

print("==== IT IS STARTED======")


print("===== RAW LIST======")
rawlist = ["State->TN~City->Chennai", "State->Kerala~City->Trivandrum"]
print(rawlist)

print()
print("===== RDD LIST======")
rddlist = sc.parallelize(rawlist)
print(rddlist.collect())

print()
print("==========flat rdd list==========")
flat = rddlist.flatMap(lambda x: x.split("~"))
print(flat.collect())

print()
print("==========state rdd list=========")
state = flat.filter(lambda x: 'State' in x)
print(state.collect())

print()
print("============= state replace ========")
staterep = state.map(lambda x: x.replace("State->", ""))
print(staterep.collect())

print()
print("============== city rdd list =========")
city = flat.filter(lambda x: 'City' in x)
print(city.collect())

print()
print("============= city replace list ======")
cityrep = city.map(lambda x: x.replace("City->", ""))
print(cityrep.collect())

sc.stop()
