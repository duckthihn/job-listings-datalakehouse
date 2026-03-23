import json
import io
from datetime import datetime
from jobs.crawlers.base_crawler import BaseCrawler
from minio import Minio
import os

class MinioIngestion:
    def __init__(self):
        endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.client = Minio(
            endpoint.replace("http://", "").replace("https://", ""),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minio_password"),
            secure=endpoint.startswith("https://"),
        )
        self.bucket_name = "data-lake"

        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

    def ingest_json_bronze(self, source, jobs_list):
        if not jobs_list:
            return
        
        now = datetime.now()
        object_name = f"{source}/{now.strftime('%Y-%m-%d')}/{now.strftime('%H-%M-%S')}.json"

        data_bytes = json.dumps(jobs_list, ensure_ascii=False).encode('utf-8')
        data_stream = io.BytesIO(data_bytes)

        try:
            self.client.put_object(
                self.bucket_name,
                object_name,
                data_stream,
                length=len(data_bytes),
                content_type="application/json"
            )
            print(f"Ingested {len(jobs_list)} records to {self.bucket_name}/{object_name}")
        except Exception as e:
            print(f"Failed to ingest data: {e}")
