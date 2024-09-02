import sys
import os
import urllib
import ssl
from pyspark.sql import SparkSession
from collections import namedtuple

# Set up environment variables
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-22'

# Create SparkSession and configure settings
spark = SparkSession.builder \
    .appName("Read") \
    .master("local[*]") \
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
    .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Read data from text file
data = sc.textFile("dt.txt")

# Split data by commas
mapsplit = data.map(lambda p: p.split(","))

# Define a namedtuple for the schema
Transaction = namedtuple('Transaction', ['txnno', 'txndate', 'amount', 'category', 'product', 'spendby'])

# Convert to RDD of namedtuples
schema_rdd = mapsplit.map(lambda p: Transaction(p[0], p[1], p[2], p[3], p[4], p[5]))

# Filter transactions where the product contains "Gymnastics"
product_filter = schema_rdd.filter(lambda p: p.product and "Gymnastics" in p.product)

# Convert to DataFrame
df = product_filter.toDF()

# Show the DataFrame
df.show()

sc.stop()
