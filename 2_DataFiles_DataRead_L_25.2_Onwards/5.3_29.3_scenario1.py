
import os
import sys

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

from pyspark.sql.functions import  *

data = [
    ('A', 'D', 'D'),
    ('B', 'A', 'A'),
    ('A', 'D', 'A')
]

df = spark.createDataFrame(data).toDF("TeamA", "TeamB", "Won")

df.show()

df1 = df

df2 = df

c = df1.groupBy('won').agg(count('won')).withColumnRenamed("won","TeamA")
c.show()

ch = df1.drop("TeamB").join(c , "TeamA", "full").drop("won")
ch.show()

f = (ch.groupBy("TeamA").agg(count('count(won)').alias('Won'))
     .withColumnRenamed("TeamA","TeamName").orderBy("TeamName"))
f.show()

sc.stop();