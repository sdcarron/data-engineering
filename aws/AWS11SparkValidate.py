import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.master ("local").appName ("SparkValidate").getOrCreate ()

basedir = os.sep.join ([os.getcwd (), "AWS", "11-DevelopmentLifecycleForPyspark", "Data"])

dataoriginal = spark.read.json (os.sep.join ([basedir, "In", "2021-01-13-0.json.gz"]))

dataoutput = spark.read.parquet (os.sep.join ([basedir, "Out", "year=2021", "month=1", "day=13", "part-00000-a2b5be3b-2ab8-418f-8869-4aa0611a54b1.c000.snappy.parquet"]))

print (dataoriginal.count ())

print (dataoutput.count ())