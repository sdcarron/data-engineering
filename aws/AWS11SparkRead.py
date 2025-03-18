import os

def SparkReadFrom (spark, directory, filename, fileformat):
    print (f"{os.sep.join ([directory, filename])}")
    filename = ".".join ([filename, fileformat, "gz"])
    df = spark.read.format (fileformat).load (f"{os.sep.join ([directory, filename])}")
    return df