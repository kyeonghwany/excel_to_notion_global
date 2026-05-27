import time
import requests
import pandas as pd
from typing import Any, Dict, List, Optional

MAX_RETRIES = 6
INITIAL_BACKOFF_SEC = 1.0
MAX_BACKOFF_SEC = 32.0


def _notion_headers(token: str, notion_version: str = "2025-09-03") -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": notion_version,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _request_with_retry(
    method: str,
    url: str,
    headers: Dict[str, str],
    *,
    json: Optional[Dict[str, Any]] = None,
    timeout: float = 60.0,
    max_retries: int = MAX_RETRIES,
) -> requests.Response:
    """429/5xx 응답에 대해 지수 백오프로 재시도. Retry-After 헤더를 우선 사용."""
    delay = INITIAL_BACKOFF_SEC
    res: Optional[requests.Response] = None

    for attempt in range(1, max_retries + 1):
        try:
            res = requests.request(
                method, url, headers=headers, json=json, timeout=timeout
            )
        except requests.RequestException as exc:
            if attempt == max_retries:
                raise
            print(
                f"[Notion] network error on {method} {url}: {exc} — "
                f"retry {attempt}/{max_retries} after {delay:.1f}s"
            )
            time.sleep(delay)
            delay = min(delay * 2, MAX_BACKOFF_SEC)
            continue

        if res.status_code != 429 and not (500 <= res.status_code < 600):
            return res

        if attempt == max_retries:
            return res

        retry_after_raw = res.headers.get("Retry-After")
        try:
            wait = float(retry_after_raw) if retry_after_raw is not None else delay
        except ValueError:
            wait = delay

        print(
            f"[Notion] {res.status_code} on {method} {url} — "
            f"retry {attempt}/{max_retries} after {wait:.1f}s"
        )
        time.sleep(wait)
        delay = min(delay * 2, MAX_BACKOFF_SEC)

    return res  # type: ignore[return-value]


def query_notion_data_source_filtered(
    data_source_id: str,
    token: str,
    filter_payload: Dict[str, Any],
    *,
    notion_version: str = "2025-09-03",
    page_size: int = 100,
    sort_payload: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    url = f"https://api.notion.com/v1/data_sources/{data_source_id}/query"
    headers = _notion_headers(token, notion_version)

    results: List[Dict[str, Any]] = []
    has_more = True
    start_cursor = None

    while has_more:
        body: Dict[str, Any] = {"page_size": page_size, "filter": filter_payload}
        if sort_payload:
            body["sorts"] = sort_payload
        if start_cursor:
            body["start_cursor"] = start_cursor

        r = _request_with_retry("POST", url, headers, json=body, timeout=60)

        if not r.ok:
            try:
                err = r.json()
            except Exception:
                err = r.text
            raise requests.HTTPError(f"{r.status_code} {r.reason}: {err}", response=r)

        data = r.json()
        results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    return results


def _prop_to_simple_value(prop: Dict[str, Any]) -> Any:
    ptype = prop.get("type")

    if ptype == "title":
        return "".join(t.get("plain_text", "") for t in prop["title"])

    if ptype == "rich_text":
        return "".join(t.get("plain_text", "") for t in prop["rich_text"])

    if ptype == "number":
        return prop.get("number")

    if ptype == "select":
        sel = prop.get("select")
        return sel.get("name") if sel else None

    if ptype == "status":
        st = prop.get("status")
        return st.get("name") if st else None

    if ptype == "date":
        d = prop.get("date")
        if not d:
            return None

        start = d.get("start")
        end = d.get("end")
        tz = d.get("time_zone")

        if end:
            return {
                "start": start,
                "end": end,
                "time_zone": tz,
            }

        return start

    return prop

def notion_pages_to_dataframe(pages: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for page in pages:
        row = {
            "page_id": page.get("id"),
            "created_time": page.get("created_time"),
            "last_edited_time": page.get("last_edited_time"),
            "url": page.get("url"),
        }
        for name, prop in (page.get("properties") or {}).items():
            row[name] = _prop_to_simple_value(prop)
        rows.append(row)
    return pd.DataFrame(rows)


def load_notion_df_filtered(
    data_source_id: str,
    token: str,
    filter_payload: Dict[str, Any],
    *,
    notion_version: str = "2025-09-03",
) -> pd.DataFrame:
    pages = query_notion_data_source_filtered(
        data_source_id=data_source_id,
        token=token,
        filter_payload=filter_payload,
        notion_version=notion_version,
    )
    return notion_pages_to_dataframe(pages)
