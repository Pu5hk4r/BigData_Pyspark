
#!!!!!!!!!!!!!!!!!!!!!!!!!--CODE IS NOT WORKING---!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

import os
import urllib.request

data_dir = "data_fROM_7.2"
os.makedirs(data_dir, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
}

for url, path in urls_and_paths.items():
    urllib.request.urlretrieve(url, path)

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import os

# Set up environment variables
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-22'

# Configure Spark
conf = SparkConf() \
    .setAppName("pyspark") \
    .setMaster("local[*]") \
    .set("spark.driver.host", "localhost") \
    .set("spark.default.parallelism", "4") \
    .set("spark.executor.memory", "4g") \
    .set("spark.driver.memory", "4g") \
    .set("spark.sql.shuffle.partitions", "4")

# Create SparkContext
sc = SparkContext(conf=conf)

# Create SparkSession
spark = SparkSession.builder.config(conf=conf).getOrCreate()



spark.read.format("csv").load("data_fROM_7.2/test.txt").toDF("Success").show(20, False)


#----------reading processing multiple file----

# Make sure this is imported
from collections import namedtuple

# Example file1 processing
file1 = sc.textFile("file1.txt", 1)
gymdata = file1.filter(lambda x: 'Gymnastics' in x)
mapsplit = gymdata.map(lambda x: x.split(","))

# Define namedtuple with the correct schema
schema = namedtuple("schema", ["txnno", "txndate", "custno", "amount", "category", "product", "city", "state", "spendby"])

# Map RDD into namedtuple structure
schemardd = mapsplit.map(lambda x: schema(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8]))

# Filter data based on 'Gymnastics' product
prodfilter = schemardd.filter(lambda x: 'Gymnastics' in x.product)

# Convert the filtered RDD into a DataFrame
schemadf = prodfilter.toDF()

# Show the resulting DataFrame
schemadf.show(5)


csvdf = spark.read.format("csv").option("header", "true").load("file3.txt")
jsondf = spark.read.format("json").load("file4.json")
parquetdf = spark.read.load("file5.parquet")

#-------- DATAFRAME-UNION----------
collist = ["txnno", "txndate", "custno", "amount", "category", "product", "city", "state", "spendby"]
schemadf1 = schemadf.select(*collist)
csvdf1 = csvdf.select(*collist)
jsondf1 = jsondf.select(*collist)
parquetdf1 = parquetdf.select(*collist)

uniondf = schemadf1.union(csvdf1).union(jsondf1).union(parquetdf1)
uniondf.show(5)

#--------PROCESSING UNIFIE-DATA---------------

procdf = (uniondf.withColumn("txndate", expr("split(txndate,'-')[2]"))
          .withColumnRenamed("txndate", "year")
          .withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
          .filter("txnno > 50000"))
procdf.show(10)
