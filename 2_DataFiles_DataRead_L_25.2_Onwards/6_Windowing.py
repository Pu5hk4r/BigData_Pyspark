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
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors" , "G1 Young Generation") \
    .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

print("----------------Getting-Started-----------------------------\n\n")
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, rank
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName("Window Function Example").getOrCreate()

# Sample data
data = [
    ("Alice", "Sales", 5000),
    ("Bob", "Sales", 6000),
    ("Charlie", "HR", 3000),
    ("David", "HR", 4000),
    ("Eve", "Sales", 7000)
]

# Create DataFrame
df = spark.createDataFrame(data, ["name", "department", "salary"])

# Define window specification
window_spec = Window.partitionBy("department").orderBy("salary")

# Apply rank and cumulative sum functions
df = df.withColumn("rank", rank().over(window_spec)) \
    .withColumn("cumulative_salary", sum("salary").over(window_spec))

# Show the results
df.show()
