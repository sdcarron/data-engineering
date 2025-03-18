from pyspark.sql import SparkSession
'''
spark = SparkSession.builder.master ("local").appName ("Blah Blah").getOrCreate ()

#print (type (spark))
spark.sql ("select current_date").show ()
'''

def spark_get (environment, name):
    envdictionary = {"DEV":"local", "PROD":"yarn"}
    spark = SparkSession.builder.master (envdictionary [environment]).appName (name).getOrCreate ()
    return spark