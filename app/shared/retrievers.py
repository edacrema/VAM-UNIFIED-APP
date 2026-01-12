"""Retrievers per news e documenti (usati da market_monitor e mfi_drafter)."""
import os
import logging
import requests
import time
import uuid
import json
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Sequence
from bs4 import BeautifulSoup
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

class GDELTRetriever:
    BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
    MAX_RETRIES = 3
    HEADERS = {'User-Agent': 'Mozilla/5.0'}
    DEFAULT_ECON_TERMS: Sequence[str] = (
        "market",
        "prices",
        "food prices",
        "food price",
        "inflation",
        "currency",
        "exchange rate",
        "trade",
        "livelihood",
        "fuel price",
        "wage",
    )
    DEFAULT_ECON_THEMES: Sequence[str] = (
        "EPU_ECONOMY",
        "EPU_POLICY",
        "TAX_ECON_PRICE",
        "ECON_STOCKMARKET",
        "WB_698_TRADE",
        "WB_1920_FINANCIAL_SECTOR_DEVELOPMENT",
        "WB_435_AGRICULTURE_AND_FOOD_SECURITY",
        "AGRICULTURE",
    )


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

    @staticmethod
    def _escape_query_value(value: str) -> str:
        return (value or "").replace('"', "\\\"").strip()

    @classmethod
    def _format_or_group(cls, terms: Sequence[str]) -> str:
        cleaned: List[str] = []
        for t in terms:
            t = (t or "").strip()
            if not t:
                continue
            safe = cls._escape_query_value(t)
            if re.search(r"\s", safe):
                cleaned.append(f'"{safe}"')
            else:
                cleaned.append(safe)
        if not cleaned:
            return ""
        if len(cleaned) == 1:
            return cleaned[0]
        return "(" + " OR ".join(cleaned) + ")"

    @classmethod
    def build_country_clause(cls, country: str) -> str:
        c = (country or "").strip()
        if not c:
            return ""
        safe = cls._escape_query_value(c)
        tokens = [t for t in re.findall(r"[^\W\d_]+", safe, flags=re.UNICODE) if t]

        if len(tokens) <= 1:
            token = tokens[0] if tokens else safe
            token_safe = cls._escape_query_value(token)
            return f'repeat2:"{token_safe}"'

        near = " ".join(tokens)
        near_safe = cls._escape_query_value(near)
        phrase = f'"{safe}"'
        near_clause = f'near5:"{near_safe}"'
        and_clause = "(" + " AND ".join(tokens) + ")"
        return f"({phrase} OR {near_clause} OR {and_clause})"

    @classmethod
    def build_economy_query(
        cls,
        country: str,
        extra_terms: Optional[Sequence[str]] = None,
        themes: Optional[Sequence[str]] = None,
    ) -> str:
        country_clause = cls.build_country_clause(country)

        merged_terms = list(cls.DEFAULT_ECON_TERMS)
        if extra_terms:
            merged_terms.extend(list(extra_terms))
        topic_clause = cls._format_or_group(merged_terms)

        merged_themes = list(themes) if themes else []
        theme_clause = ""
        theme_items = [f"theme:{t}" for t in merged_themes if (t or "").strip()]
        if theme_items:
            theme_clause = "(" + " OR ".join(theme_items) + ")"

        focus_clause = ""
        if topic_clause and theme_clause:
            focus_clause = f"({topic_clause} OR {theme_clause})"
        elif topic_clause:
            focus_clause = topic_clause
        elif theme_clause:
            focus_clause = theme_clause

        if country_clause and focus_clause:
            return f"{country_clause} {focus_clause}".strip()
        if country_clause:
            return country_clause
        return focus_clause


    def fetch(self, query: str, start_date: str, end_date: str, max_records: int = 5) -> List[Dict]:
        started = time.time()
        now_utc = datetime.utcnow()
        trace: Dict[str, Any] = {
            "retriever": "GDELT",
            "query": query,
            "start_date": start_date,
            "end_date": end_date,
            "max_records": max_records,
            "attempts": 0,
            "status_code": None,
            "request_url": None,
            "content_type": None,
            "response_snippet": None,
            "fallback_used": False,
            "fallback_query": None,
            "num_articles": 0,
            "num_documents": 0,
            "samples": [],
            "error": None,
            "duration_ms": None,
        }

        start_dt_raw = None
        end_dt_raw = None
        try:
            start_dt_raw = datetime.strptime(start_date[:10], "%Y-%m-%d")
        except Exception:
            start_dt_raw = None
        try:
            end_dt_raw = datetime.strptime(end_date[:10], "%Y-%m-%d")
        except Exception:
            end_dt_raw = None

        if start_dt_raw is not None and start_dt_raw > now_utc:
            trace["error"] = "start_date is in the future"
            trace["duration_ms"] = int((time.time() - started) * 1000)
            self.last_trace = trace
            if self.verbose:
                logger.info(json.dumps({"gdelt_trace": trace}, ensure_ascii=False))
            return []

        start_dt_param = self._format_datetime(start_date)
        end_dt_param = self._format_datetime(end_date)
        if end_dt_raw is not None and end_dt_raw.date() > now_utc.date():
            end_dt_param = now_utc.strftime("%Y%m%d%H%M%S")

        if start_dt_raw is not None and end_dt_raw is not None and end_dt_raw < start_dt_raw:
            trace["error"] = "end_date is before start_date"
            trace["duration_ms"] = int((time.time() - started) * 1000)
            self.last_trace = trace
            if self.verbose:
                logger.info(json.dumps({"gdelt_trace": trace}, ensure_ascii=False))
            return []

        base_query = f"{query} sourcelang:english"

        params = {
            "query": base_query,
            "mode": "artlist",
            "maxrecords": max_records,
            "format": "json",
            "sort": "DateDesc",
            "startdatetime": start_dt_param,
            "enddatetime": end_dt_param
        }

        def run_once(p: Dict[str, Any]) -> tuple[List[Dict[str, Any]], Optional[str]]:
            resp = requests.get(self.BASE_URL, params=p, timeout=30, headers=self.HEADERS)
            trace["status_code"] = resp.status_code
            trace["request_url"] = getattr(resp, "url", None)
            trace["content_type"] = (resp.headers or {}).get("Content-Type")
            trace["response_snippet"] = None

            if resp.status_code >= 400:
                snippet = (resp.text or "").strip()
                trace["response_snippet"] = (snippet[:2000] if snippet else "")
                return [], (snippet[:2000] if snippet else f"HTTP {resp.status_code}")

            try:
                data = resp.json() if resp.text.strip() else {}
            except Exception as e:
                trace["response_snippet"] = (resp.text or "")[:2000]
                return [], f"Failed to parse JSON response: {str(e)}"

            if isinstance(data, dict) and data.get("error"):
                return [], str(data.get("error"))[:2000]

            articles = data.get("articles", [])
            trace["num_articles"] = len(articles) if isinstance(articles, list) else 0
            documents: List[Dict[str, Any]] = []
            for art in articles if isinstance(articles, list) else []:
                content = self._scrape_content(art.get("url"))
                publisher = art.get("domain", "")
                safe_content = content if content else (art.get("title", "") or "")
                documents.append({
                    "doc_id": f"gdelt_{uuid.uuid4().hex[:8]}",
                    "title": art.get("title", ""),
                    "url": art.get("url", ""),
                    "source": "GDELT",
                    "publisher": publisher,
                    "date": art.get("seendate", "")[:8],
                    "content": safe_content[:5000],
                })
            return documents, None

        documents: List[Dict[str, Any]] = []
        last_error: Optional[str] = None
        for attempt in range(self.MAX_RETRIES):
            trace["attempts"] = attempt + 1
            try:
                resp = requests.get(self.BASE_URL, params=params, timeout=30, headers=self.HEADERS)
                trace["status_code"] = resp.status_code
                trace["request_url"] = getattr(resp, "url", None)
                trace["content_type"] = (resp.headers or {}).get("Content-Type")
                trace["response_snippet"] = None

                if resp.status_code == 429:
                    time.sleep(2 ** attempt)
                    continue

                if resp.status_code >= 400:
                    snippet = (resp.text or "").strip()
                    trace["response_snippet"] = (snippet[:2000] if snippet else "")
                    last_error = (snippet[:2000] if snippet else f"HTTP {resp.status_code}")
                    break

                try:
                    data = resp.json() if resp.text.strip() else {}
                except Exception as e:
                    trace["response_snippet"] = (resp.text or "")[:2000]
                    last_error = f"Failed to parse JSON response: {str(e)}"
                    break

                if isinstance(data, dict) and data.get("error"):
                    last_error = str(data.get("error"))[:2000]
                    break

                articles = data.get("articles", [])
                trace["num_articles"] = len(articles) if isinstance(articles, list) else 0
                for art in articles if isinstance(articles, list) else []:
                    content = self._scrape_content(art.get("url"))
                    publisher = art.get("domain", "")
                    safe_content = content if content else (art.get("title", "") or "")
                    documents.append({
                        "doc_id": f"gdelt_{uuid.uuid4().hex[:8]}",
                        "title": art.get("title", ""),
                        "url": art.get("url", ""),
                        "source": "GDELT",
                        "publisher": publisher,
                        "date": art.get("seendate", "")[:8],
                        "content": safe_content[:5000],
                    })

                if documents:
                    last_error = None
                break
            except Exception as e:
                last_error = str(e)
                time.sleep(1)

        if not documents and 'repeat2:"' in base_query:
            relaxed_q = re.sub(r'repeat2:"([^"]+)"', r'\1', base_query)
            if relaxed_q != base_query:
                trace["fallback_used"] = True
                trace["fallback_query"] = relaxed_q
                fallback_params = dict(params)
                fallback_params["query"] = relaxed_q
                documents, last_error = run_once(fallback_params)

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
        trace["error"] = last_error
        trace["duration_ms"] = int((time.time() - started) * 1000)
        self.last_trace = trace
        if self.verbose:
            logger.info(json.dumps({"gdelt_trace": trace}, ensure_ascii=False))
        return documents


class ReliefWebRetriever:
    BASE_URL = "https://api.reliefweb.int/v2/reports"
    DEFAULT_ECON_TERMS: Sequence[str] = (
        "market",
        "prices",
        "food prices",
        "food price",
        "inflation",
        "currency",
        "exchange rate",
        "trade",
        "livelihood*",
        "fuel price",
        "wage",
    )


    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.last_trace: Dict[str, Any] = {}

    @classmethod
    def build_economy_query(cls, extra_terms: Optional[Sequence[str]] = None) -> str:
        merged_terms = list(cls.DEFAULT_ECON_TERMS)
        if extra_terms:
            merged_terms.extend(list(extra_terms))

        cleaned: List[str] = []
        for t in merged_terms:
            t = (t or "").strip()
            if not t:
                continue
            safe = t.replace('"', "\\\"")
            if re.search(r"\s", safe):
                cleaned.append(f'"{safe}"')
            else:
                cleaned.append(safe)

        if not cleaned:
            return ""
        if len(cleaned) == 1:
            return cleaned[0]
        return "(" + " OR ".join(cleaned) + ")"

    def fetch(
        self,
        country: str,
        start_date: str,
        end_date: str,
        max_records: int = 10,
        query: Optional[str] = None,
    ) -> List[Dict]:
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

        appname = (os.getenv("RELIEFWEB_APPNAME") or "").strip()
        if not appname:
            trace["error"] = "Missing RELIEFWEB_APPNAME (ReliefWeb API requires appname in URL)"
            trace["duration_ms"] = int((time.time() - started) * 1000)
            self.last_trace = trace
            if self.verbose:
                logger.info(json.dumps({"reliefweb_trace": trace}, ensure_ascii=False))
            return []

        start_iso = f"{start_date[:10]}T00:00:00+00:00"
        end_iso = f"{end_date[:10]}T23:59:59+00:00"
        payload = {
            "filter": {
                "operator": "AND",
                "conditions": [
                    {"field": "country", "value": country},
                    {"field": "date.created", "value": {"from": start_iso, "to": end_iso}}
                ]
            },
            "limit": max_records,
            "fields": {"include": ["title", "url", "source", "date.created", "body"]},
            "sort": ["date.created:desc"],
        }
        q = (query or "").strip()
        if q:
            payload["query"] = {
                "value": q,
                "operator": "OR",
                "fields": ["title", "body"],
            }
        try:
            resp = requests.post(
                self.BASE_URL,
                params={"appname": appname},
                json=payload,
                timeout=30,
                headers={"User-Agent": f"{appname} (python requests)"},
            )

            trace["status_code"] = resp.status_code

            if resp.status_code >= 400:
                snippet = (resp.text or "").strip()
                if snippet:
                    trace["error"] = snippet[:2000]
                else:
                    trace["error"] = f"HTTP {resp.status_code}"
                trace["duration_ms"] = int((time.time() - started) * 1000)
                self.last_trace = trace
                if self.verbose:
                    logger.info(json.dumps({"reliefweb_trace": trace}, ensure_ascii=False))
                return []

            try:
                parsed = resp.json() if resp.text.strip() else {}
            except Exception as e:
                trace["error"] = f"Failed to parse JSON response: {str(e)}"
                trace["duration_ms"] = int((time.time() - started) * 1000)
                self.last_trace = trace
                if self.verbose:
                    logger.info(json.dumps({"reliefweb_trace": trace}, ensure_ascii=False))
                return []

            data = parsed.get("data", []) if isinstance(parsed, dict) else []
            documents = []
            for item in data:
                fields = item.get("fields", {})
                date_created = ""
                if isinstance(fields, dict):
                    if isinstance(fields.get("date"), dict):
                        date_created = (fields.get("date") or {}).get("created", "")
                    if not date_created:
                        date_created = fields.get("date.created", "")
                body_val = fields.get("body", "") if isinstance(fields, dict) else ""
                if body_val is None:
                    body_val = ""
                if not isinstance(body_val, str):
                    body_val = json.dumps(body_val, ensure_ascii=False)
                documents.append({
                    "doc_id": f"rw_{item.get('id', '')}",
                    "title": fields.get("title", ""),
                    "url": fields.get("url", ""),
                    "source": "ReliefWeb",
                    "date": (date_created or "")[:10],
                    "content": body_val[:5000]
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