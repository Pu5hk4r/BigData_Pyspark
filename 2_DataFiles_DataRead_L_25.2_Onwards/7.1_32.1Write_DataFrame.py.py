import sys
import os

# Set Python and Java paths
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-22'  # Make sure the Java version is compatible with PySpark
#os.environ['HADOOP_HOME'] = "F:\Hadoop"


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

csv_df = spark.read.format("csv").option("header","true").load("usdata.csv")
csv_df.show()

#file_df = csv_df.filter("state = 'LA'") # called sql style this is working fine but not recommended

from pyspark.sql.functions import col
file_df = csv_df.filter(col("state") == "LA")

file_df.show()

file_df.write \
    .format("json") \
    .mode("overwrite") \
    .save("7.1_usdata_writed_json")


    #.save("file:///F:/data/jwrite")  #worked sucessfully


# write always be on above with file variable and partfile generated and by default parquet file generated eventhough format is json
#instead of path we can create folder in our working directory as writed -> save("writed') hence no path ..part file would be generated in inside the folder
#write formula is save for everyehere kafka mongodb



