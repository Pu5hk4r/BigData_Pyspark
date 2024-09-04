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

data = [     #explicitly creating data
    "00000000,06-26-2011,200,Exercise,GymnasticsPro,cash",
    "00000001,05-26-2011,300,Exercise,Weightlifting,credit",
    "00000002,06-01-2011,100,Exercise,GymnasticsPro,cash",
    "00000003,06-05-2011,100,Gymnastics,Rings,credit",
    "00000004,12-17-2011,300,Team Sports,Field,paytm",
    "00000005,02-14-2011,200,Gymnastics,,cash"
]

# Convert data to RDD
rdd = spark.sparkContext.parallelize(data)

columns = ["txnno", "txndate", "amount", "category", "product", "spendby"]

# Convert RDD to DataFrame with the specified column names
csvdf = rdd.map(lambda x: x.split(",")).toDF(columns)

csvdf.show()

print("\n\n___________________Proc Dataframe-----------------------\n\n")

procdf = (
    csvdf.selectExpr(
        "cast(txnno as int) as txnno",  #  removing zeroes  from txnno-column ----column select
        "split(txndate , '-') [2] as year",  # removing date and month --> txndate ---column select
        "amount + 100 as amount",  # add +100   Expression
        "upper(category) as category",  # make column attribute uppercase Expression
        "lower(product) as product",  #  make column attribute lowercase Expression
        "spendby",  #   created new column
        """case 
        when spendby = 'cash' then 0 
        when spendby = 'paytm' then 2 
        else 1 end 
        as status"""
    )
)

procdf.show()

procdf.printSchema()
