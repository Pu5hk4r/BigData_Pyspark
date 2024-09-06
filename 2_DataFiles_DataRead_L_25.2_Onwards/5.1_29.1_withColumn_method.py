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


print("\n\n___________________withcolumn1-Expresions-----------------------\n\n")

#to using withcolumn method has to import from pyspark.sql.functions import *

from pyspark.sql.functions import *
procdf = (
    csvdf.withColumn("category" , expr("upper(category)"))
         .withColumn("amount" , expr("amount+100"))
         .withColumn("txnno",expr("cast(txnno as int)"))
         .withColumn("txndate",expr("split(txndate,'-')[2]"))
         .withColumn("product",expr("lower(product)"))
         .withColumn("spendby",expr("spendby"))
         .withColumn("status",expr("case when spendby = 'cash' then 0 else 1 end")) # case when spendby= 'cash' then 0 else 1 end as status
         .withColumn("concate",expr("concat(category, '~Push')"))


)

procdf.show()
print("\n\n--------------2withColumns-------------------------------------------\n\n")

procdff  = (
              csvdf.withColumns({

                  "txnno" : expr("cast(txnno as int)"),
                  "txndate":expr("split(txndate,'-')[2]"),
              })
)

procdff.show()
print("\n\n--------------withColumnRenamed-------------------------------------------\n\n")
#in dt.txt we changed txndate column name with year name and date reduced  years using selecExpr
#bub now using withcolumn method it creating new column named as year
#csvdf.withColumn("year",expr("split(txndate , '_')[2]---wrong
# we have to use withcolumnRenamed

procdf = (

    csvdf.withColumn("txndate" , expr("split(txndate, '-')[2]"))
         .withColumnRenamed("txndate" , "year")
)
procdf.show()
