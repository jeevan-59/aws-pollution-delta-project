import requests
from bs4 import BeautifulSoup
import pandas as pd
import boto3
from io import StringIO

def handler(event, context):
    s3 = boto3.resource('s3')
    BUCKET = "plltndelta-landing-layer"


    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36',
    }
    response = requests.get("https://aqicn.org/scale/", headers=headers)

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find('table', class_="infoaqitable cautionary")
    dataset = []

    for row in table.find_all('tr')[1:]:

        data = {
            'aqi_from'                       : row.find_all('td')[0].text.replace('+', '-').split('-')[0].strip(),
            'aqi_to'                         : row.find_all('td')[0].text.replace('+', '-').split('-')[1].strip(),
            'Air_Pollution_Level'            : row.find_all('td')[1].text,
            'Health_Implications'            : row.find_all('td')[2].text,
            'Cautionary_Statement_for_PM2.5' : row.find_all('td')[3].text,
        }

        dataset.append(data)

    df = pd.DataFrame(data=dataset)
    print(df)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False,quoting=1 , sep=";")
    #df.to_csv('air_quality_and_pollution_measurement.csv', index=False,quoting=1 , sep=";")
    #s3.Bucket(BUCKET).upload_file("air_quality_and_pollution_measurement.csv", "aqpm/air_quality_and_pollution_measurement.csv")
    s3.Object(BUCKET, 'aqpm/air_quality_and_pollution_measurement.csv').put(Body=csv_buffer.getvalue())