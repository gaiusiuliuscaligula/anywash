import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

# Получаем access_token из переменных окружения
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Определяем даты (вчера)
yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
date_from = f"{yesterday} 00:00:00"
date_till = f"{yesterday} 23:59:59"

def get_calls_report(access_token, date_from, date_till, offset=0, limit=10000):
    url = "https://dataapi.uiscom.ru/v2.0"
    headers = {"Content-Type": "application/json"}

    fields = [
        "id", "start_time", "finish_time", "virtual_phone_number", "is_transfer",
        "finish_reason", "direction", "source", "communication_number",
        "communication_page_url", "communication_id", "communication_type", "is_lost",
        "cpn_region_id", "cpn_region_name", "cpn_country_name",
        "wait_duration", "total_wait_duration", "lost_call_processing_duration",
        "talk_duration", "clean_talk_duration", "total_duration", "postprocess_duration",
        "ua_client_id", "ym_client_id", "sale_date", "sale_cost", "search_query",
        "search_engine", "referrer_domain", "referrer", "entrance_page", "gclid",
        "yclid", "ymclid", "ef_id", "channel", "site_id", "site_domain_name",
        "campaign_id", "campaign_name", "auto_call_campaign_name", "visit_other_campaign",
        "visitor_id", "person_id", "visitor_type", "visitor_session_id", "visits_count",
        "visitor_first_campaign_id", "visitor_first_campaign_name", "visitor_city",
        "visitor_region", "visitor_country", "visitor_device", "last_answered_employee_id",
        "last_answered_employee_full_name", "last_answered_employee_rating",
        "first_answered_employee_id", "first_answered_employee_full_name", "scenario_id",
        "scenario_name", "call_api_external_id", "call_api_request_id",
        "contact_phone_number", "contact_full_name", "contact_id", "utm_source",
        "utm_medium", "utm_term", "utm_content", "utm_campaign", "openstat_ad",
        "openstat_campaign", "openstat_service", "openstat_source", "eq_utm_source",
        "eq_utm_medium", "eq_utm_term", "eq_utm_content", "eq_utm_campaign",
        "eq_utm_referrer", "eq_utm_expid", "operator_phone_number", "source_id",
        "source_name", "source_new", "channel_new", "channel_code", "ext_id", "properties"
    ]

    nested_fields = [
        "recognized_text", "tags", "visitor_custom_properties",
        "segments", "employees", "scenario_operations", "call_records", "voice_mail_records"
    ]

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

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        result = response.json()
        if "result" in result and "data" in result["result"]:
            return result["result"]["data"]
        else:
            print("Ошибка в ответе API:", result)
            return []
    else:
        print("Ошибка запроса:", response.status_code, response.text)
        return []

def process_calls_data(calls):
    df = pd.DataFrame(calls)
    
    # Разворачиваем вложенный список `employees`
    if 'employees' in df.columns:
        df["is_talked"] = df["employees"].apply(
            lambda x: ", ".join([str(emp.get("is_talked", "")) for emp in x]) if isinstance(x, list) else "")
        df["employee_id"] = df["employees"].apply(
            lambda x: ", ".join([str(emp.get("employee_id", "")) for emp in x]) if isinstance(x, list) else "")
        df["is_answered"] = df["employees"].apply(
            lambda x: ", ".join([str(emp.get("is_answered", "")) for emp in x]) if isinstance(x, list) else "")
        df["employee_full_name"] = df["employees"].apply(
            lambda x: ", ".join([str(emp.get("employee_full_name", "")) for emp in x]) if isinstance(x, list) else "")
        df.drop(columns=["employees"], inplace=True)
    
    return df

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    client = bigquery.Client()
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Данные загружены в {table_ref}")

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
    calls = get_calls_report(ACCESS_TOKEN, date_from, date_till)
    
    if calls:
        df = process_calls_data(calls)
        upload_to_bigquery(df, "deep-wave-449812-r5", "anywash_data", "calls")
    else:
        print("Нет данных для загрузки")
