import os
from dotenv import load_dotenv

load_dotenv()

AWS_BUCKET_NAME = "financial-market-lakehouse-bruno"
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

SOURCE_NAME = "alpha_vantage"
DATASET_NAME = "daily"

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "TSLA"]