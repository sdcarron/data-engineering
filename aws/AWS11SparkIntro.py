from pyspark.sql import SparkSession
import AWS11SparkUtil
import AWS11SparkRead
import AWS11SparkProcess
import AWS11SparkWrite
import os

def main ():
    environment = os.environ.get ("ENVIRONMENT")
    print (f"environment = {environment}")
    
    spark = AWS11SparkUtil.spark_get (environment, "blah blah")#SparkSession.builder.master ("local").appName ("Blah Blah").getOrCreate ()
    
    #print (type (spark))
    #spark.sql ("select current_date").show ()
    indirectory = os.environ.get ("INDIRECTORY")
    infile = f"{os.environ.get ('INFILE')}"
    informat = os.environ.get ("INFORMAT")

    print ("Directory = ", indirectory)
    print ("FilePattern = ", infile)
    print ("FileFormat = ", informat)
    
    df = AWS11SparkRead.SparkReadFrom (spark, indirectory, infile, informat)
    df = AWS11SparkProcess.SparkProcessTransform (df)
    df.printSchema ()
    df.select ("repo.*", "created_at", "year", "month", "day").show ()

    outdirectory = os.environ.get ("OUTDIRECTORY")
    outfile = ""
    outformat = os.environ.get ("OUTFORMAT")
    AWS11SparkWrite.SparkWriteOut (df, outdirectory, outformat)

if __name__ == "__main__":
    main ()