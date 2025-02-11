import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

# Конфигурация API UIS
ACCESS_TOKEN = "6rk603f45cviuh1jkuubgb8xiwm5bxmrp3r5w6qg"
UIS_API_URL = "https://dataapi.uiscom.ru/v2.0"

# Конфигурация BigQuery
BQ_PROJECT_ID = "your-gcp-project-id"  # Твой GCP проект
BQ_DATASET_ID = "your_dataset"  # Твой датасет в BigQuery
BQ_TABLE_ID = "calls"  # Таблица, куда загружаем данные

# Функция для запроса звонков за прошлый день
def get_calls_report(date_from, date_till, offset=0, limit=10000):
    headers = {"Content-Type": "application/json"}
    fields = ["id", "start_time", "finish_time", "virtual_phone_number", "finish_reason", "direction", "talk_duration"]
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

    response = requests.post(UIS_API_URL, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        result = response.json()
        return result.get("result", {}).get("data", [])
    else:
        print(f"Ошибка API: {response.status_code} {response.text}")
        return []

# Функция для загрузки данных в BigQuery
def upload_to_bigquery(data):
    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_ref = client.dataset(BQ_DATASET_ID).table(BQ_TABLE_ID)

    # Преобразуем в DataFrame
    df = pd.DataFrame(data)

    if df.empty:
        print("Нет данных для загрузки в BigQuery.")
        return

    # Загружаем в BigQuery
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Дождаться завершения

    print(f"Загружено {len(df)} записей в {BQ_TABLE_ID}")

# Основная функция для Cloud Functions
def main(event, context):
    yesterday = datetime.utcnow() - timedelta(days=1)
    date_from = yesterday.strftime("%Y-%m-%d 00:00:00")
    date_till = yesterday.strftime("%Y-%m-%d 23:59:59")

    print(f"Запрашиваем звонки с {date_from} по {date_till}")
    calls = get_calls_report(date_from, date_till)

    if calls:
        upload_to_bigquery(calls)
    else:
        print("Нет данных за этот день.")
