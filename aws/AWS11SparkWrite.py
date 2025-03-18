import pyspark.sql


def SparkWriteOut (df, location, format):
    df.coalesce (16).write.partitionBy ("year", "month", "day").mode ("append").format (format).save (location)