import io
import json
import os
import re
from datetime import datetime
from html import unescape

from curl_cffi import requests
from minio import Minio


class BaseCrawler:
    def __init__(self, source: str):
        self.source = source
        endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minio_password")
        self.bucket_name = "data-lake"
        self.client = Minio(
            endpoint.replace("http://", "").replace("https://", ""),
            access_key=access_key,
            secret_key=secret_key,
            secure=endpoint.startswith("https://"),
        )
        self.session = requests.Session()

    def crawl(self):
        raise NotImplementedError

    def fetch_html(self, url: str) -> str:
        response = self.session.get(
            url,
            impersonate="chrome",
            timeout=30,
            headers={
                "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
            },
        )
        response.raise_for_status()
        return response.text

    @staticmethod
    def strip_tags(value: str) -> str:
        text = re.sub(r"<[^>]+>", " ", value)
        text = unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def unique_by_url(jobs_list):
        deduped = []
        seen = set()
        for job in jobs_list:
            url = job.get("url")
            if not url or url in seen:
                continue
            seen.add(url)
            deduped.append(job)
        return deduped

    def upload(self, jobs_list):
        if not jobs_list:
            print(f"No jobs collected for source '{self.source}'")
            return

        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

        now = datetime.utcnow()
        object_name = f"{self.source}/{now.strftime('%Y-%m-%d')}/{now.strftime('%H-%M-%S')}.json"
        payload = json.dumps(jobs_list, ensure_ascii=False).encode("utf-8")

        self.client.put_object(
            self.bucket_name,
            object_name,
            io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        print(f"Uploaded {len(jobs_list)} records to s3a://{self.bucket_name}/{object_name}")

    def run(self):
        self.upload(self.crawl())
