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

rdd1 = spark.sparkContext.parallelize([

    (1,'Pushkar'),
    (2,'Parashar'),
    (3,'SharmaG'),
    (4,'vivek')
],1)

rdd2 = spark.sparkContext.parallelize([

    (1,'Mouse'),
    (3,'Mobile'),
    (7,'Laptop')
],1)

#Convert RDDS to Datframes using toDF()
#Defines schemas for the Dataframes

df1 = rdd1.toDF(['id' ,'name']).coalesce(1)
df2 = rdd2.toDF(['id' , 'product']).coalesce(1)

df1.show()
df2.show()
print("\n----------------INNER-JOINS-------------------------------------------------\n")

inner = df1.join( df2  , ["id"] , "inner")
inner.show()

print("\n----------------LEFT-JOINS-------------------------------------------------\n")

left = df1.join( df2  , ["id"] , "left")
left.show()


print("\n----------------RIGHT-JOINS-------------------------------------------------\n")

right = df1.join( df2  , ["id"] , "right")
right.show()

print("\n----------------FULL-JOINS-------------------------------------------------\n")

full = df1.join( df2  , ["id"] , "full")
full.show()