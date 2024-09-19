import sys
import os

# Set Python and Java paths
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-22'  # Make sure the Java version is compatible with PySpark

from pyspark.sql import SparkSession

# Create SparkSession and configure settings
spark = SparkSession.builder \
    .appName("Read CSV Files") \
    .master("local[*]") \
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
    .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

print("----------------Getting-Started-----------------------------\n\n")

from pyspark.sql.functions import *

# Sample data
data = [("DEPT3", 500),
        ("DEPT3", 200),
        ("DEPT1", 1000),
        ("DEPT1", 700),
        ("DEPT1", 500),
        ("DEPT2", 400),
        ("DEPT2", 200)]
columns = ["dept", "salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

df.show()

from pyspark.sql.window import Window

# Define Window
deptwindow = Window.partitionBy("dept").orderBy(col("salary").desc())

# Apply window with Dense Rank
dfrank = df.withColumn("drank", dense_rank().over(deptwindow))

dfrank.show()

# Filter rows where the dense rank is 2
filterdf = dfrank.filter("drank=2")

filterdf.show()

# Drop the drank column
finaldf = filterdf.drop("drank")

finaldf.show()

print("\n-----------------------Cast-Function-------------------------------\n")
print("=======agg df and used cast function too=======")
print()

from pyspark.sql.types import *

# Create a sample DataFrame for aggregation (you can replace this with procdf if you define it elsewhere)
procdf = spark.createDataFrame([("A", 100.50), ("A", 200.75), ("B", 300.20)], ["category", "amount"])

# Perform aggregation and casting
aggdf = procdf.groupby("category").agg(sum("amount").alias("total"))

# Use the cast function to convert total to IntegerType
aggdf = aggdf.withColumn("total", col("total").cast(IntegerType()))  # CAST FUNCTION

aggdf.show()
