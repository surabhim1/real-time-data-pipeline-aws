import json
import boto3
import urllib.request
from datetime import datetime

def lambda_handler(event, context):
    API_KEY = "32736f6fbaf7b96fe732df13b5c91d1b"
    CITY = "London"
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"

    try:
        with urllib.request.urlopen(url) as response:
            weather_data = json.loads(response.read().decode())

        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%SZ')
        weather_data['timestamp'] = timestamp

        s3 = boto3.client('s3')
        s3.put_object(
            Bucket='weather-raw-data2',  # ✅ Your actual bucket name
            Key=f"weather/raw/{CITY}_{timestamp}.json",
            Body=json.dumps(weather_data)
        )

        return {
            "statusCode": 200,
            "body": "Success"
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }
