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

print("-----------------RAW Data-----------------------------------------------------------\n\n")

# Load data
data = sc.textFile("state.txt")
print(data.collect())  # If you want to print the content of the file

data.foreach(print)


print()

print("--------------------------------Raw data----------------------------")

data2 =  sc.textFile("usdata.csv")
print(data2.collect())

print()
print("--------------------------------Len data----------------------------")

lendata = data2.filter(lambda p : len(p) > 200)

print(lendata.collect())

# Stop the SparkContext
sc.stop()
