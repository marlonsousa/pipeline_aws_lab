from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import boto3
import io
import pandas as pd
import json

def conectar_mongo(uri):
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    return client

def create_connect_dbmongo(client, db_name):
    db = client[db_name]
    return db

def create_connect_collection(db, collection_name):
    collection = db[collection_name]
    return collection

def request_api(url):
    return requests.get(url).json()
    
def upload_s3_json(bucket_name, caminho_arquivo_s3, data_api):
    from botocore.exceptions import NoCredentialsError, ClientError
    try:

        # Cliente S3 (credenciais já devem estar configuradas)
        s3 = boto3.client("s3")

        # Converte o dict que veio da API em json
        json_str = json.dumps(data_api, indent=4, ensure_ascii=False)

        # Envia o JSON como objeto para o S3
        s3.put_object(
            Bucket=bucket_name,
            Key=caminho_arquivo_s3,
            Body=json_str,
            ContentType="application/json"
        )

        return("Arquivo JSON enviado com sucesso para o S3!")
    except NoCredentialsError:
        return("Credenciais da AWS não configuradas.")
    except ClientError as e:
        return("Erro do cliente:", e.response["Error"]["Message"])

def upload_s3_parquet(bucket_name, caminho_arquivo_s3, data):
    # 2. Converter para Parquet em memória
    buffer = io.BytesIO()
    data.to_parquet(buffer, engine='pyarrow', index=False)

    # 3. Subir para o S3
    s3 = boto3.client("s3")

    # 4. Enviar para o S3
    s3.put_object(
        Bucket=bucket_name,
        Key=caminho_arquivo_s3,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )

if __name__=='__main__':
    url_api = "https://www.alphavantage.co/query?function=CASH_FLOW&symbol=alterarCliente&apikey=JR2BDPA9HPKPSMBY"
    url_mongo = "mongodb+srv://marlon:12345@cluster-pipeline.dqret56.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-pipeline"
    client = conectar_mongo(url_mongo)
    db = create_connect_dbmongo(client, "db_fundamentaldata")
    collection = create_connect_collection(db, "cash_flow")
    lista_empresas_api = ["IBM", "MSFT", "GOOGL", "NVDA"]
    for name in lista_empresas_api:
        data = request_api(url_api.replace("alterarCliente", name))
        collection.replace_one(
            {"symbol": data["symbol"]},  # filtro
            data,                                          # novo documento
            upsert=True                                    # insere se não existir
        )
        upload_s3_json("bucketmgslab",f"cash_flow/raw_data/json/{data["symbol"]}.json",data)
        df_annual = pd.json_normalize(data["annualReports"])
        df_quarter = pd.json_normalize(data["quarterlyReports"])
        df_annual.insert(loc=0, column="cliente", value=data["symbol"])
        df_quarter.insert(loc=0, column="cliente", value=data["symbol"])
        upload_s3_parquet("bucketmsglabconsumo", f"parquet/reporte_anual/{df_annual["cliente"][0]}.parquet", df_annual)
        upload_s3_parquet("bucketmsglabconsumo", f"parquet/reporte_quarter/{df_quarter["cliente"][0]}.parquet", df_quarter)

    client.close()