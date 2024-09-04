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

print("----------------Getting-Started-----------------------------\n\n")
csvdf = spark.read.format("csv").load("dt.txt").toDF("txnno","txndate","amount","category","product","spendby") # to assign columns toDF used

csvdf.show()


print("\n\n___________________Proc Dataframe-----------------------\n\n")

procdf = (
             csvdf.selectExpr(            #performing expression operation then use selectExpr
                          "txnno",   #column select#
                          "txndate",    #column select#  #othervise it will treated as column_name
                          "amount+100 as amount", #Expression# #used as amount so that column name dont change
                          "upper (category) as category",#Expression#
                          "product (lower) as product", #column select
                          "spendby" #column select
             )
)

procdf.show()
