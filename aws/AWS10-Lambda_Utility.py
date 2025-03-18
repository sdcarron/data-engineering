from datetime import datetime as dt
from datetime import timedelta as td
import lambda_download
import json
import requests

basefile = "2021-01-29-0.json.gz"

basedt = basefile.split (".") [0]

print (dt.strptime (basedt, "%Y-%d-%M-%H"))
print (dt.strptime (basedt, "%Y-%d-%M-%H") + td (hours=1))
print (dt.strftime (dt.strptime (basedt, "%Y-%d-%M-%H") + td (hours=1), "%Y-%d-%M-%#H"))

'''
for  i in range (24):
    print (i)
'''
prefix = "https://data.gharchive.org/"
#i = 0
#while i <= 24:
process = True
while process == True:
    print (basefile)
    baseresponse = requests.get (prefix + basefile)#lambda_download.lambda_downloader (prefix + basefile)
    print (baseresponse.status_code)
    if baseresponse.status_code == 200:
        basesplit = basefile.split (".")
        basedate = basesplit [0]
        basesuffix = ".".join (basesplit [1:len (basesplit)])
        baseparse = dt.strptime (basedate, "%Y-%M-%d-%H")
        baseincrement = baseparse + td (hours=1)
        basefile = ".".join ([dt.strftime (baseincrement, "%Y-%M-%d-%#H"), basesuffix])
        #print (i)
        #i = i+1
    else:
        process = False