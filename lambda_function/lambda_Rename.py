import json
import requests
import boto3
from datetime import datetime

def lambda_handler(event, context):
    API_KEY = "32736f6fbaf7b96fe732df13b5c91d1b"
    CITY = "London"
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"

    response = requests.get(url)
    weather_data = response.json()
    weather_data['timestamp'] = datetime.utcnow().isoformat()

    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='weather-raw-data2',
        Key=f"weather/raw/{CITY}_{weather_data['timestamp']}.json",
        Body=json.dumps(weather_data)
    )

    return {"statusCode": 200, "body": "Success"}

