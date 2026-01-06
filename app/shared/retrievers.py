"""Retrievers per news e documenti (usati da market_monitor e mfi_drafter)."""
import requests
import time
import uuid
import json
from datetime import datetime
from typing import List, Dict
from bs4 import BeautifulSoup


class GDELTRetriever:
    BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
    MAX_RETRIES = 3
    HEADERS = {'User-Agent': 'Mozilla/5.0'}

    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def _format_datetime(self, date_str: str) -> str:
        try:
            dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            return dt.strftime("%Y%m%d%H%M%S")
        except ValueError:
            return date_str.replace("-", "")[:8] + "000000"

    def _scrape_content(self, url: str) -> str:
        if not url:
            return ""
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=10)
            if resp.status_code != 200:
                return ""
            soup = BeautifulSoup(resp.text, 'lxml')
            paragraphs = soup.find_all('p')
            text = ' '.join([p.get_text() for p in paragraphs])
            return text.strip()[:5000]
        except Exception:
            return ""

    def fetch(self, query: str, start_date: str, end_date: str, max_records: int = 5) -> List[Dict]:
        params = {
            "query": f"{query} sourcelang:english",
            "mode": "artlist",
            "maxrecords": max_records,
            "format": "json",
            "sort": "DateDesc",
            "startdatetime": self._format_datetime(start_date),
            "enddatetime": self._format_datetime(end_date)
        }
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = requests.get(self.BASE_URL, params=params, timeout=30)
                if resp.status_code == 429:
                    time.sleep(2 ** attempt)
                    continue
                data = resp.json() if resp.text.strip() else {}
                articles = data.get("articles", [])
                documents = []
                for art in articles:
                    content = self._scrape_content(art.get("url"))
                    if len(content) >= 150:
                        documents.append({
                            "doc_id": f"gdelt_{uuid.uuid4().hex[:8]}",
                            "title": art.get("title", ""),
                            "url": art.get("url", ""),
                            "source": art.get("domain", "GDELT"),
                            "date": art.get("seendate", "")[:8],
                            "content": content
                        })
                return documents
            except Exception:
                time.sleep(1)
        return []


class ReliefWebRetriever:
    BASE_URL = "https://api.reliefweb.int/v1/reports"

    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def fetch(self, country: str, start_date: str, end_date: str, max_records: int = 10) -> List[Dict]:
        payload = {
            "filter": {
                "operator": "AND",
                "conditions": [
                    {"field": "country.name", "value": country},
                    {"field": "date.created", "value": {"from": start_date, "to": end_date}}
                ]
            },
            "limit": max_records,
            "fields": {"include": ["title", "url", "source", "date.created", "body"]}
        }
        try:
            resp = requests.post(self.BASE_URL, json=payload, timeout=30)
            data = resp.json().get("data", [])
            documents = []
            for item in data:
                fields = item.get("fields", {})
                documents.append({
                    "doc_id": f"rw_{item.get('id', '')}",
                    "title": fields.get("title", ""),
                    "url": fields.get("url", ""),
                    "source": "ReliefWeb",
                    "date": fields.get("date", {}).get("created", "")[:10],
                    "content": fields.get("body", "")[:5000]
                })
            return documents
        except Exception:
            return []