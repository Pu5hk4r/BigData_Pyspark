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


print("\n\n___________________Select txndate product-----------------------\n\n")

select_df = csvdf.select("txndate","product")
select_df.show()

print("\n\n___________________Drop txndate product-----------------------\n\n")

drop_df = csvdf.drop("txndate","product")
drop_df.show()

print("\n\n___________________Category = 'Exercise'-----------------------\n\n")


single_fil = csvdf.filter(" category = 'Exercise ' ")

single_fil.show()

print("\n\n___________________Category = 'Exercise' and spendby = cash-----------------------\n\n")

double_fil = csvdf.filter(" category = 'Exercise' and spendby = 'cash' ")

double_fil.show()

print("\n\n___________________Category = 'Exercise' or  spendby = cash-----------------------\n\n")

double_fil_or = csvdf.filter(" category = 'Exercise' or spendby = 'cash' ")

double_fil_or.show()

print("\n\n------------category = exercise  as well as Gymnastics--------------------\n\n")

multi_value = csvdf.filter(" category in  ('Exercise', 'Gymnstics')")
multi_value.show()

print("\n\n------------Product contains Gymnastics--------------------\n\n")

like_fil = csvdf.filter("product like '%Gymnastics%'  ")
like_fil.show()

print("\n\n------------Product is NULL--------------------\n\n")

null_fil = csvdf.filter("product is null ")
null_fil.show()

print("\n\n------------Product is  not NULL--------------------\n\n")

non_null_fil = csvdf.filter("product is not null ")
non_null_fil.show()

print("\n\n------------Category not equal to Exercise--------------------\n\n")

not_fil = csvdf.filter("category  != 'Exercise' ")
not_fil.show()

print("\n\n------------Category = Exercise and spendby != cash --------------------\n\n")

eq_not_fil = csvdf.filter("category  = 'Exercise'  and spendby != 'cash' ")
eq_not_fil.show()


