import json
import os
import sys
from datetime import datetime

import requests

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from configs.config import (  # noqa: E402
    ALPHA_VANTAGE_API_KEY,
    AWS_BUCKET_NAME,
    DATASET_NAME,
    SOURCE_NAME,
    SYMBOLS,
)
from src.utils.s3_helper import upload_file_to_s3  # noqa: E402


def build_alpha_vantage_url(symbol: str, api_key: str) -> str:
    return (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY"
        f"&symbol={symbol}"
        f"&apikey={api_key}"
    )


def build_s3_key(symbol: str, extraction_date: str) -> str:
    file_name = f"{symbol.lower()}_{DATASET_NAME}.json"
    return (
        f"raw/{SOURCE_NAME}/{DATASET_NAME}/"
        f"symbol={symbol}/"
        f"extraction_date={extraction_date}/"
        f"{file_name}"
    )


def fetch_market_data(symbol: str) -> dict:
    url = build_alpha_vantage_url(symbol, ALPHA_VANTAGE_API_KEY)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


def save_local_json(data: dict, local_file_path: str) -> None:
    with open(local_file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def main():
    if not ALPHA_VANTAGE_API_KEY:
        raise ValueError("ALPHA_VANTAGE_KEY não encontrada no .env")

    extraction_date = datetime.utcnow().strftime("%Y-%m-%d")
    os.makedirs("tmp", exist_ok=True)

    for symbol in SYMBOLS:
        print(f"Ingerindo {symbol}...")

        data = fetch_market_data(symbol)

        enriched_data = {
            "metadata": {
                "source": SOURCE_NAME,
                "dataset": DATASET_NAME,
                "symbol": symbol,
                "extraction_date": extraction_date,
                "ingestion_timestamp_utc": datetime.utcnow().isoformat(),
            },
            "payload": data,
        }

        local_file_path = f"tmp/{symbol.lower()}_{DATASET_NAME}.json"
        save_local_json(enriched_data, local_file_path)

        s3_key = build_s3_key(symbol, extraction_date)

        upload_file_to_s3(
            local_file_path=local_file_path,
            bucket_name=AWS_BUCKET_NAME,
            s3_key=s3_key,
        )

        print(f"Upload concluído: s3://{AWS_BUCKET_NAME}/{s3_key}")

    print("Ingestão finalizada com sucesso.")


if __name__ == "__main__":
    main()