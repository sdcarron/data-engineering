import os
import boto3

os.environ ["BUCKET_NAME"] = "udemy-github-awsbucket"

def lambda_uploader (service):
    return boto3.client (service)

def lambda_upload (client, bucket, key, input):
    output = client.put_object (Bucket=bucket, Key=key, Body=input)
    return output


'''
s3client = boto3.client ("s3")

s3objects = s3client.list_objects (Bucket="udemy-github-awsbucket")

print (s3objects)

from lambda_download import lambda_downloader
file = "2021-01-29-0.json.gz"
response = lambda_downloader (file)

s3upload = s3client.put_object (Bucket="udemy-github-awsbucket", Key=file, Body=response.content)

print (s3upload)
'''