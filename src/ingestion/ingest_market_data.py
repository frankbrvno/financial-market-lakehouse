import requests
import boto3
import json
from datetime import datetime
import os

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL&apikey={API_KEY}"

response = requests.get(url)

data = response.json()

# salvar local
file_name = f"aapl_{datetime.now().strftime('%Y%m%d')}.json"

with open(file_name, "w") as f:
    json.dump(data, f)

# enviar para S3
s3 = boto3.client("s3")

bucket = "financial-market-lakehouse-bruno"

s3.upload_file(
    file_name,
    bucket,
    f"raw/{file_name}"
)

print("Upload concluído")