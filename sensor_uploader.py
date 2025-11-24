# sensor_uploader.py
import os
import json
import time
import random
import logging
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from time import sleep

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Config (use environment variables)
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
S3_BUCKET = os.getenv("S3_BUCKET", "env-monitor-data")
RETRY_MAX = 5

# Create S3 client (reads creds from env or ~/.aws/credentials)
s3 = boto3.client('s3', region_name=AWS_REGION)

def generate_reading():
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
        "pm2_5": round(random.uniform(5.0, 200.0), 2),
        "co2": round(random.uniform(300.0, 1000.0), 2)
    }

def upload_json(bucket, key, payload):
    body = json.dumps(payload)
    attempt = 0
    while attempt < RETRY_MAX:
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=body)
            logging.info("Uploaded %s", key)
            return True
        except ClientError as e:
            attempt += 1
            logging.warning("Upload failed (attempt %d/%d): %s", attempt, RETRY_MAX, str(e))
            sleep(2 ** attempt)  # exponential backoff
    logging.error("Failed to upload %s after %d attempts", key, RETRY_MAX)
    return False

if __name__ == "__main__":
    interval = int(os.getenv("UPLOAD_INTERVAL_SECONDS", "5"))
    while True:
        data = generate_reading()
        key = f"sensor-data/{data['timestamp']}.json"
        upload_json(S3_BUCKET, key, data)
        time.sleep(interval)
