import requests
import boto3
import json 
import datetime
import pytz

BUCKET = "plltndelta-landing-layer"
CITY = 'sofia'
TOKEN = 'f646bb0e24e5bbbfca2c684a8d758e6af5397752'
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36',
}

def gather_ids(resp):
    ids = []
    if not (resp.json().get('data') is None):
            for cid in resp.json()['data']:
                if not (cid.get('uid') is None):
                    ids.append(f"@{cid['uid']}")
    
    return ids

def save_to_s3(cityids) :
    s3 = boto3.resource('s3')
    jsonApi = f'https://api.waqi.info/feed/<<IDX>>/?token={TOKEN}'

    dt_today = datetime.datetime.today()   # Local time
    dt_Sofia = dt_today.astimezone(pytz.timezone('Europe/Sofia')) 
    timestr = dt_Sofia.strftime("%Y-%m-%d--%H-%M-%S")

    for cityid in set(cityids):
        jsResp = requests.get(jsonApi.replace('<<IDX>>', cityid), headers=headers)
        file_name = f"{timestr}_{cityid}.json"
        #print(file_name)
        s3.Object(BUCKET, f'{CITY}_area/{file_name}').put(Body=json.dumps(jsResp.json(), ensure_ascii=False).encode('utf-8'))

def handler(event, context):
    cityids = []
    searchUrl = f'https://api.waqi.info/search/?token={TOKEN}&keyword={CITY}'

    response  = requests.get(searchUrl, headers=headers)
    print(response.url)

    cityids.extend(gather_ids(response))

    save_to_s3(cityids)
