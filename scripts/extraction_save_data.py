import sys
import os

# Adiciona o diretório pai ao sys.path para importar config.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import config

import requests
import boto3
import io
import pandas as pd
import json
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from botocore.exceptions import NoCredentialsError, ClientError

# ---------- MONGODB ----------
def conectar_mongo(uri: str) -> MongoClient:
    """
    Conecta ao MongoDB e retorna o client.
    """
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Conectado ao MongoDB!")
    except Exception as e:
        print("Erro ao conectar ao MongoDB:", e)
    return client

def get_db(client: MongoClient, db_name: str):
    return client[db_name]

def get_collection(db, collection_name: str):
    return db[collection_name]


# ---------- API ----------
def request_api(url: str) -> dict:
    """
    Realiza request à API e retorna o JSON.
    """
    return requests.get(url).json()


# ---------- UPLOAD S3 ----------
def upload_s3_json(bucket: str, path: str, data: dict) -> str:
    """
    Converte um dict em JSON e sobe pro S3.
    """
    try:
        s3 = boto3.client("s3")
        json_str = json.dumps(data, indent=4, ensure_ascii=False)

        s3.put_object(
            Bucket=bucket,
            Key=path,
            Body=json_str,
            ContentType="application/json"
        )
        return "JSON enviado com sucesso para o S3!"
    except NoCredentialsError:
        return "Credenciais da AWS não configuradas."
    except ClientError as e:
        return f"Erro do cliente: {e.response['Error']['Message']}"

def upload_s3_parquet(bucket: str, path: str, df: pd.DataFrame):
    """
    Converte um DataFrame em Parquet e envia para o S3.
    """
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)

        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=path,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream"
        )
        return "Parquet enviado com sucesso para o S3!"
    except NoCredentialsError:
        return "Credenciais da AWS não configuradas."
    except ClientError as e:
        return f"Erro do cliente: {e.response['Error']['Message']}"

# ---------- MAIN ----------
if __name__ == "__main__":
    # Configurações
    url_template = f"https://www.alphavantage.co/query?function=CASH_FLOW&symbol={{simbolo}}&apikey={config.API_KEY}"
    empresas = ["IBM", "MSFT", "GOOGL", "NVDA"]

    client = conectar_mongo(config.MONGO_URI)
    db = client["db_fundamentaldata"]
    collection = db["cash_flow"]

    for nome in empresas:
        print(f"\n🔍 Processando: {nome}")
        url = url_template.format(simbolo=nome)
        data = request_api(url)

        # MongoDB: upsert
        collection.replace_one({"symbol": data["symbol"]}, data, upsert=True)

        # Upload JSON bruto
        upload_s3_json(config.S3_BUCKET_RAW, f"cash_flow/raw_data/json/{data['symbol']}.json", data)

        # Upload Parquet dos relatórios
        df_annual = pd.json_normalize(data.get("annualReports", []))
        df_quarter = pd.json_normalize(data.get("quarterlyReports", []))

        for df in [df_annual, df_quarter]:
            df.insert(0, "cliente", data["symbol"])

        upload_s3_parquet(config.S3_BUCKET_PARQUET, f"parquet/reporte_anual/{data['symbol']}.parquet", df_annual)
        upload_s3_parquet(config.S3_BUCKET_PARQUET, f"parquet/reporte_quarter/{data['symbol']}.parquet", df_quarter)

    client.close()
    print("\n✅ Processo finalizado com sucesso.")