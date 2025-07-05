# config.py

import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
API_KEY = os.getenv("API_KEY")
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW")
S3_BUCKET_PARQUET = os.getenv("S3_BUCKET_PARQUET")