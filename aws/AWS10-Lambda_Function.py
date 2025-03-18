import os
import json
import lambda_download
import lambda_upload
import lambda_bookmark

os.environ ["PREFIX"] = "sandbox"

def lambda_handler (event, context):
    # TODO implement
    environ = os.environ.get ("ENVIRON")
    if environ == "DEV":
        os.environ.setdefault ("AWS_DEFAULT", "udemy-github-profile")
    bucket = os.environ.get ("BUCKET_NAME")
    bookmark = os.environ.get ("BOOKMARK_FILE")
    baseline = os.environ.get ("BASELINE_FILE")
    prefix = os.environ.get ("PREFIX")
    
    while True:
        prevfile = lambda_bookmark.lambda_bookmarkprev ()
        nextfile = lambda_bookmark.lambda_bookmarknext ()
        downfile = lambda_download.lambda_downloader (nextfile)

        if downfile.status_code != 200:
            break
    
    
    file = "2021-01-29-1.json.gz"
    download = lambda_download.lambda_downloader (file)
    uploader = lambda_upload.lambda_uploader ("s3")
    #print ("Just made the uploader")
    result = lambda_upload.lambda_upload (uploader, os.environ.get ("BUCKET_NAME"), f"{prefix}/{file}", download.content)
    #print ("Just produced the result from uploading")
    '''
    return {
        'statusCode': response.status_code,#200,
        'body': json.dumps ("Download status code")#json.dumps('Hello from Lambda! (In the lambda project)')
    }
    '''
    print (result)
    return result
