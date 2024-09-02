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

#from push import spark

conf = SparkConf().setAppName("Read").setMaster("local[*]")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")  #Pushkar added extra ERROR,WARN,INFO ,DEBUG


spark = SparkSession.builder.getOrCreate()

print("==== IT IS STARTED======")

a = 2

print(a)

b  = a + 2

print(b)

c= "zeyobron"

print(c)

d= [ 1 , 2 , 3]


print()
print("========= RAW INT LIST========")
print()

print(d)

print()
print("========= RDD INT LIST========")
print()

rddint = sc.parallelize(d)
print(rddint.collect())


print()
print("========= aADD INT LIST========")
print()

add =  rddint.map( lambda x : x  +  2)
print(add.collect())

print()
print("========= Mul INT LIST========")
print()

mul  = rddint.map( lambda x : x * 10)
print(mul.collect())



print()
print("========= fil INT LIST========")
print()

fillist = rddint.filter( lambda x : x > 2)
print(fillist.collect())



print()
print("=========Raw str========")
print()

rawstr = ["zeyobron" , "zeyo" , "analytics" ]
print(rawstr)



print()
print("=========Raw rdd str========")
print()


rddstr = sc.parallelize(rawstr)
print(rddstr.collect())

print()
print("=========concat rdd str========")
print()

addstr= rddstr.map(lambda x : x + " pvt")

print(addstr.collect())

print()
print("=========rep rdd str========")
print()

repstr = rddstr.map(lambda x : x.replace("zeyo","tera"))
print(repstr.collect())


print()
print("=========fil rdd str========")
print()

filstr = rddstr.filter(lambda  x : 'zeyo' in x )
print(filstr.collect())

print()
print("=========FLAT raw str========")
print()

rawflat = sc.parallelize(["A~B" , "C~D"])
print(rawflat.collect())



print()
print("=========flatt rdd str========")
print()

flat = rawflat.flatMap( lambda x : x.split("~"))
print(flat.collect())

sc.stop()