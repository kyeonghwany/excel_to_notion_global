import requests
import pandas as pd
from typing import Any, Dict, List, Optional


def _notion_headers(token: str, notion_version: str = "2025-09-03") -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": notion_version,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


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

        r = requests.post(url, headers=headers, json=body, timeout=60)

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
