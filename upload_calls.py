import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta

# Константы
BQ_PROJECT_ID = "deep-wave-449812-r5"
BQ_DATASET_ID = "anywash_data"
BQ_TABLE_ID = "calls"
UIS_TOKEN = os.getenv("6rk603f45cviuh1jkuubgb8xiwm5bxmrp3r5w6qg")
HEADERS = {"Authorization": f"Bearer {UIS_TOKEN}"}

def get_calls_report(date_from, date_till):
    url = "https://api.uiscom.ru/calls/report/v1"
    payload = {
        "date_from": date_from,
        "date_till": date_till,
        "fields": ["id", "start_time", "finish_time", "virtual_phone_number", "finish_reason", "direction", "talk_duration"]
    }
    response = requests.post(url, headers=HEADERS, json=payload)
    
    if response.status_code == 200:
        data = response.json()
        if "data" in data:
            return data["data"]
        else:
            print("Пустой ответ от API UIS")
            return []
    else:
        print(f"Ошибка API UIS: {response.status_code}, {response.text}")
        return []

def upload_to_bigquery(data):
    client = bigquery.Client()
    table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
    df = pd.DataFrame(data)
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("start_time", "TIMESTAMP"),
            bigquery.SchemaField("finish_time", "TIMESTAMP"),
            bigquery.SchemaField("virtual_phone_number", "STRING"),
            bigquery.SchemaField("finish_reason", "STRING"),
            bigquery.SchemaField("direction", "STRING"),
            bigquery.SchemaField("talk_duration", "INTEGER"),
        ]
    )
    
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Загружено {len(df)} строк в {table_ref}")
    except Exception as e:
        print(f"Ошибка загрузки в BigQuery: {e}")

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_key.json"
    
    date_yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    calls = get_calls_report(date_yesterday, date_yesterday)
    
    if calls:
        print(f"Получено {len(calls)} записей")
        print(json.dumps(calls[:5], indent=2, ensure_ascii=False))  # Вывод первых 5 записей
        upload_to_bigquery(calls)
    else:
        print("Нет данных для загрузки")
