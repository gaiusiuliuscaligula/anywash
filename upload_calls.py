import requests
import json
import pandas as pd
import os
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Читаем Google Credentials из GitHub Secrets (переменной окружения)
gcp_credentials = os.getenv("GCP_SERVICE_ACCOUNT")

# Записываем креды во временный файл (BigQuery требует файл)
if gcp_credentials:
    creds_path = "/tmp/gcp_credentials.json"
    with open(creds_path, "w") as f:
        f.write(gcp_credentials)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    logging.info("✅ GCP Credentials загружены.")
else:
    logging.error("❌ Ошибка: GCP_SERVICE_ACCOUNT не найден!")
    exit(1)

# Конфигурация API UIS
ACCESS_TOKEN = os.getenv("UIS_ACCESS_TOKEN")  # Токен из GitHub Secrets
UIS_API_URL = "https://dataapi.uiscom.ru/v2.0"

# Конфигурация BigQuery
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")  # Проект GCP
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")  # Датасет в BigQuery
BQ_TABLE_ID = "calls"  # Таблица для загрузки звонков

# Функция для запроса звонков
def get_calls_report(date_from, date_till):
    headers = {"Content-Type": "application/json"}
    fields = ["id", "start_time", "finish_time", "virtual_phone_number", "finish_reason", "direction", "talk_duration"]
    calls = []
    offset = 0
    limit = 1000  # Ограничение API

    while True:
        payload = {
            "jsonrpc": "2.0",
            "id": "1",
            "method": "get.calls_report",
            "params": {
                "access_token": ACCESS_TOKEN,
                "date_from": date_from,
                "date_till": date_till,
                "offset": offset,
                "limit": limit,
                "fields": fields
            }
        }

        response = requests.post(UIS_API_URL, headers=headers, json=payload)

        if response.status_code != 200:
            logging.error(f"❌ Ошибка API: {response.status_code} {response.text}")
            return []

        try:
            result = response.json()
            data = result.get("result", {}).get("data", [])
        except json.JSONDecodeError:
            logging.error("❌ Ошибка парсинга JSON из API UIS!")
            return []

        if not data:
            break  # Данные закончились

        calls.extend(data)
        offset += limit

    logging.info(f"✅ Получено {len(calls)} звонков.")
    return calls

# Функция загрузки данных в BigQuery
def upload_to_bigquery(data):
    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_ref = client.dataset(BQ_DATASET_ID).table(BQ_TABLE_ID)

    df = pd.DataFrame(data)

    if df.empty:
        logging.warning("⚠️ Нет данных для загрузки в BigQuery.")
        return

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    df["finish_time"] = pd.to_datetime(df["finish_time"], errors="coerce")
    df["talk_duration"] = pd.to_numeric(df["talk_duration"], errors="coerce")

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logging.info(f"✅ Загружено {len(df)} записей в BigQuery ({BQ_TABLE_ID}).")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки в BigQuery: {str(e)}")

# Основная функция для GitHub Actions
def main():
    yesterday = datetime.utcnow() - timedelta(days=1)
    date_from = yesterday.strftime("%Y-%m-%d 00:00:00")
    date_till = yesterday.strftime("%Y-%m-%d 23:59:59")

    logging.info(f"🔍 Запрашиваем звонки с {date_from} по {date_till}")
    calls = get_calls_report(date_from, date_till)

    if calls:
        upload_to_bigquery(calls)
    else:
        logging.warning("⚠️ Нет данных за этот день.")

# Запуск
if __name__ == "__main__":
    main()
