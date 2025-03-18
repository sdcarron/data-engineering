import os
import boto3
import requests
from botocore.errorfactory import ClientError
from datetime import datetime as dt
from datetime import timedelta as td

os.environ.setdefault ("AWS_PROFILE", "udemy-github-profile")

def lambda_bookmarkclient (service):
    return boto3.client (service)


def lambda_bookmarkprev (bucket, prefix, bookmark, baseline):
    prevfile = ""
    client = lambda_bookmarkclient ("s3")
    
    try:
        prevfile = client.get_object (Bucket=bucket, Key="/".join ([prefix, bookmark]).read ().decode ("UTF-8")
    except ClientError as e:
        if e.response ["Error"]["Code"] == "NoSuchKey":
            prevfile = baseline
        else:
            raise
    return prevfile


def lambda_bookmarkupload (bucket, prefix, file, contents):
    client = lambda_bookmarkclient ("s3")
    client.put_object (Bucket=bucket, Key="/".join ([prefix, file]), Body=contents.encode ("UTF-8"))


def lambda_bookmarknext (infile):
    nextfile = ""
    nextsplit = infile.split (".")
    nextdate = nextsplit [0]
    nextsuffix = ".".join (nextsplit [1:len (nextsplit)])
    nextparse = dt.strptime (nextdate, "%Y-%M-%d-%H")
    nextincrement = nextparse + td (hours=1)
    nextfile = ".".join ([dt.strftime (nextincrement, "%Y-%M-%d-%#H"), nextsuffix])
    return nextfile
                                      
'''
bookmarkclient = boto3.client ("s3")

#bookmarkfile = "2021-01-30-0.json.gz"
bookmarkfile = dt.today ().strftime ("%Y-%m-%d") + "-0.json.gz"
print ("Just made the bookmark file string" + bookmarkfile)

bookmarkbucket = "udemy-github-awsbucket"
bookmarkkey = "sandbox/bookmark"
bookmarkboolean = True

try:
    bookmarkfile = bookmarkclient.get_object (Bucket=bookmarkbucket, Key=bookmarkkey) ["Body"].read ().decode ("UTF-8")
    #print (bookmarkfile)
    #print (bookmarkfile ["Body"]) #This shows that the Body contents is a Bytestream data type
    #print (bookmarkfile ["Body"].read ()) #Trying to display the contents of the Body contents Bytestream
    #print (bookmarkfile ["Body"].read ().decode ("UTF-8"))
except ClientError as e:
    if e.response ["Error"]["Code"] == "NoSuchKey":
        #bookmarkfile = "2021-01-30-0.json.gz"
        bookmarkfile = dt.today ().strftime ("%Y-%m-%d") + "-0.json.gz"
        bookmarkboolean = False
    else:
        raise



#client.put_object (Bucket="udemy-github-awsbucket", Key="sandbox/bookmark", Body=bookmarkfile.encode ("UTF-8"))

bookmarkprefix = "https://data.gharchive.org/"
#i = 0
#while i <= 24:
while True:

    if bookmarkboolean == True:
        print  (bookmarkfile)
        bookmarksplit = bookmarkfile.split (".")
        bookmarkdate = bookmarksplit [0]
        bookmarksuffix = ".".join (bookmarksplit [1:len (bookmarksplit)])
        bookmarkparse = dt.strptime (bookmarkdate, "%Y-%M-%d-%H")
        bookmarkincrement = bookmarkparse + td (hours=1)
        bookmarkfile = ".".join ([dt.strftime (bookmarkincrement, "%Y-%M-%d-%#H"), bookmarksuffix])

    print (bookmarkfile)
    bookmarkresponse = requests.get (bookmarkprefix + bookmarkfile)#lambda_download.lambda_downloader (prefix + basefile)
    print (bookmarkresponse.status_code)

    if bookmarkresponse.status_code != 200:
        break
        #print (i)
        #i = i+1
    else:
        bookmarkclient.put_object (Bucket=bookmarkbucket, Key=bookmarkkey, Body=bookmarkfile.encode ("UTF-8"))
        bookmarkclient.put_object (Bucket=bookmarkbucket, Key=bookmarkfile, Body=bookmarkresponse.content)
        bookmarkboolean = True
'''