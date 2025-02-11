import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

# Получаем access_token из переменных окружения
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Загружаем учетные данные GCP из секрета
gcp_credentials_json = os.getenv("GCP_CREDENTIALS_JSON")
if not gcp_credentials_json:
    raise ValueError("Не найден секрет GCP_CREDENTIALS_JSON!")

credentials = service_account.Credentials.from_service_account_info(json.loads(gcp_credentials_json))

# Определяем даты (вчера)
yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
date_from = f"{yesterday} 00:00:00"
date_till = f"{yesterday} 23:59:59"

def get_calls_report(access_token, date_from, date_till, offset=0, limit=10000):
    """Запрос данных о звонках через API UIS."""
    url = "https://dataapi.uiscom.ru/v2.0"
    headers = {"Content-Type": "application/json"}

    fields = [
        "id", "start_time", "finish_time", "virtual_phone_number", "is_transfer",
        "finish_reason", "direction", "source", "communication_number",
        "communication_page_url", "communication_id", "communication_type", "is_lost",
        "cpn_region_id", "cpn_region_name", "cpn_country_name",
        "wait_duration", "total_wait_duration", "lost_call_processing_duration",
        "talk_duration", "clean_talk_duration", "total_duration", "postprocess_duration",
        "visitor_id", "person_id", "visitor_type", "visitor_session_id", "visits_count",
        "last_answered_employee_id", "last_answered_employee_full_name",
        "first_answered_employee_id", "first_answered_employee_full_name",
        "contact_phone_number", "contact_full_name", "contact_id", "utm_source",
        "utm_medium", "utm_term", "utm_content", "utm_campaign"
    ]

    nested_fields = ["employees"]

    payload = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "get.calls_report",
        "params": {
            "access_token": access_token,
            "date_from": date_from,
            "date_till": date_till,
            "offset": offset,
            "limit": limit,
            "fields": fields + nested_fields
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        result = response.json()

        if "result" in result and "data" in result["result"]:
            return result["result"]["data"]
        else:
            print("Ошибка в ответе API:", result)
            return []

    except requests.RequestException as e:
        print(f"Ошибка запроса к API: {e}")
        return []

def process_calls_data(calls):
    """Обработка данных звонков и преобразование их в DataFrame."""
    if not calls:
        print("Нет данных для обработки.")
        return pd.DataFrame()  

    df = pd.DataFrame(calls)

    # Разворачиваем вложенные списки `employees`
    if "employees" in df.columns:
        df["is_talked"] = df["employees"].apply(lambda x: ", ".join([str(emp.get("is_talked", "")) for emp in x]) if isinstance(x, list) else "")
        df["employee_id"] = df["employees"].apply(lambda x: ", ".join([str(emp.get("employee_id", "")) for emp in x]) if isinstance(x, list) else "")
        df["is_answered"] = df["employees"].apply(lambda x: ", ".join([str(emp.get("is_answered", "")) for emp in x]) if isinstance(x, list) else "")
        df["employee_full_name"] = df["employees"].apply(lambda x: ", ".join([str(emp.get("employee_full_name", "")) for emp in x]) if isinstance(x, list) else "")
        df.drop(columns=["employees"], inplace=True)
    
    return df

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    """Загрузка данных в BigQuery."""
    if df.empty:
        print("Нет данных для загрузки в BigQuery.")
        return

    client = bigquery.Client(credentials=credentials, project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Ожидание завершения загрузки
        print(f"Данные успешно загружены в {table_ref}")
    except Exception as e:
        print(f"Ошибка загрузки данных в BigQuery: {e}")

if __name__ == "__main__":
    calls = get_calls_report(ACCESS_TOKEN, date_from, date_till)

    if calls:
        df = process_calls_data(calls)
        upload_to_bigquery(df, "deep-wave-449812-r5", "anywash_data", "calls")
    else:
        print("Нет данных для загрузки.")
