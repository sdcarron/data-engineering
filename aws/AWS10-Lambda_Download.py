import requests

def lambda_downloader (file):
    print ("Making lambda_downloader call for: " + file)
    response = requests.get (f"https://data.gharchive.org/{file}")
    return response


#response = lambdadownload_file ("2025-02-29-0.json.gz")

#print (response.status_code)