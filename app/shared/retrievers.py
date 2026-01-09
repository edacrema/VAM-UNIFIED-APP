"""Retrievers per news e documenti (usati da market_monitor e mfi_drafter)."""
import logging
import requests
import time
import uuid
import json
from datetime import datetime
from typing import List, Dict, Any
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)

class GDELTRetriever:
    BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
    MAX_RETRIES = 3
    HEADERS = {'User-Agent': 'Mozilla/5.0'}

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.last_trace: Dict[str, Any] = {}

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
            soup = BeautifulSoup(resp.text, 'html.parser')
            paragraphs = soup.find_all('p')
            text = ' '.join([p.get_text() for p in paragraphs])
            return text.strip()[:5000]
        except Exception:
            return ""

    def fetch(self, query: str, start_date: str, end_date: str, max_records: int = 5) -> List[Dict]:
        started = time.time()
        trace: Dict[str, Any] = {
            "retriever": "GDELT",
            "query": query,
            "start_date": start_date,
            "end_date": end_date,
            "max_records": max_records,
            "attempts": 0,
            "status_code": None,
            "num_articles": 0,
            "num_documents": 0,
            "samples": [],
            "error": None,
            "duration_ms": None,
        }
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
                trace["attempts"] = attempt + 1
                resp = requests.get(self.BASE_URL, params=params, timeout=30)
                trace["status_code"] = resp.status_code
                if resp.status_code == 429:
                    time.sleep(2 ** attempt)
                    continue
                data = resp.json() if resp.text.strip() else {}
                articles = data.get("articles", [])
                trace["num_articles"] = len(articles) if isinstance(articles, list) else 0
                documents = []
                for art in articles:
                    content = self._scrape_content(art.get("url"))
                    if len(content) >= 150:
                        publisher = art.get("domain", "")
                        documents.append({
                            "doc_id": f"gdelt_{uuid.uuid4().hex[:8]}",
                            "title": art.get("title", ""),
                            "url": art.get("url", ""),
                            "source": "GDELT",
                            "publisher": publisher,
                            "date": art.get("seendate", "")[:8],
                            "content": content
                        })
                trace["num_documents"] = len(documents)
                trace["samples"] = [
                    {
                        "title": d.get("title", ""),
                        "url": d.get("url", ""),
                        "date": d.get("date", ""),
                        "publisher": d.get("publisher", ""),
                    }
                    for d in documents[:3]
                ]
                trace["duration_ms"] = int((time.time() - started) * 1000)
                self.last_trace = trace
                if self.verbose:
                    logger.info(json.dumps({"gdelt_trace": trace}, ensure_ascii=False))
                return documents
            except Exception as e:
                trace["error"] = str(e)
                time.sleep(1)
        trace["duration_ms"] = int((time.time() - started) * 1000)
        self.last_trace = trace
        if self.verbose:
            logger.info(json.dumps({"gdelt_trace": trace}, ensure_ascii=False))
        return []


class ReliefWebRetriever:
    BASE_URL = "https://api.reliefweb.int/v1/reports"

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.last_trace: Dict[str, Any] = {}

    def fetch(self, country: str, start_date: str, end_date: str, max_records: int = 10) -> List[Dict]:
        started = time.time()
        trace: Dict[str, Any] = {
            "retriever": "ReliefWeb",
            "country": country,
            "start_date": start_date,
            "end_date": end_date,
            "max_records": max_records,
            "status_code": None,
            "num_documents": 0,
            "samples": [],
            "error": None,
            "duration_ms": None,
        }
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
            trace["status_code"] = resp.status_code
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

            trace["num_documents"] = len(documents)
            trace["samples"] = [
                {"title": d.get("title", ""), "url": d.get("url", ""), "date": d.get("date", "")}
                for d in documents[:3]
            ]
            trace["duration_ms"] = int((time.time() - started) * 1000)
            self.last_trace = trace
            if self.verbose:
                logger.info(json.dumps({"reliefweb_trace": trace}, ensure_ascii=False))
            return documents
        except Exception as e:
            trace["error"] = str(e)
            trace["duration_ms"] = int((time.time() - started) * 1000)
            self.last_trace = trace
            if self.verbose:
                logger.info(json.dumps({"reliefweb_trace": trace}, ensure_ascii=False))
            return []