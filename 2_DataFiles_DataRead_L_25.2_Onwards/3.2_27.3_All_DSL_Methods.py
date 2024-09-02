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

csvdf.show()
print("\n\n-----CATEGORY = EXERCISE -----\n")

onefil = csvdf.filter("       category = 'Exercise'             ")

onefil.show()

print("\n\n-----CATEGORY = EXERCISE AND SPENDBY = CASH -----\n")

mulfilter = csvdf.filter("category = 'Exercise' and spendby = 'cash'")

mulfilter.show()

print("\n\n-----CATEGORY = EXERCISE OR SPENDBY = CASH -----\n")

mulfilteror = csvdf.filter("category = 'Exercise' or spendby = 'cash'")

mulfilteror.show()

print("\n\n-----CATEGORY = EXERCISE ,GYMNASTICS -----\n")

infil = csvdf.filter("category in ('Exercise'  , 'Gymnastics')")

infil.show()

print("\n\n-----PRODUCT CONTAINS (LIKE) GYMNASTICS -----\n")

likefil = csvdf.filter(" product like   '%Gymnastics%'  ")

likefil.show()

print("\n\n-----PRODUCT IS NULL  -----\n")

nullfilter = csvdf.filter(" product is null ")

nullfilter.show()


print("\n\n-----PRODUCT IS NOT NULL  -----\n")

notnullfilter = csvdf.filter(" product is not null ")

notnullfilter.show()

print("\n\n-----MULTILINER   -----\n")

multiliner = csvdf.filter(" category = 'Exercise'").drop("product").select("txnno","txndate")

multiliner.show()