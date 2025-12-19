import requests
import pandas as pd
from datetime import datetime, date
from typing import Dict, Any, Optional

NOTION_VERSION = "2025-09-03"
BASE_URL = "https://api.notion.com/v1"


class NotionAPIError(Exception):
    """간단한 Notion API 에러 래퍼"""
    pass


def _make_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def get_data_source_schema(
    data_source_id: str,
    token: str,
) -> Dict[str, Any]:
    """
    /v1/data_sources/{data_source_id} 로부터 properties 스키마를 가져온다.
    (각 프로퍼티의 name, type 등을 사용해서 DataFrame → Notion 변환에 활용)
    """
    url = f"{BASE_URL}/data_sources/{data_source_id}"
    res = requests.get(url, headers=_make_headers(token))
    if not res.ok:
        raise NotionAPIError(
            f"Failed to retrieve data source schema: "
            f"{res.status_code} {res.text}"
        )
    data = res.json()
    # data['properties'] 는 { "Name": { "id": "...", "type": "title", ... }, ... } 형태
    return data["properties"]


def _is_null(value: Any) -> bool:
    # pandas NaN, None, 빈 문자열 등 처리
    if value is None:
        return True
    try:
        import math
        if isinstance(value, float) and math.isnan(value):
            return True
    except Exception:
        pass
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _to_iso_date(value: Any) -> str:
    """pandas Timestamp / datetime / date / str → ISO 8601 start 값으로 변환"""
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        # date만 있으면 00:00 붙여도 되고 그냥 isoformat 도 무방
        return value.isoformat()
    if isinstance(value, str):
        # 아주 러프하게: 그냥 문자열 그대로 넣어도 Notion이 처리 가능
        # (YYYY-MM-DD, YYYY-MM-DDTHH:MM 등)
        return value
    # 그 외는 문자열 캐스팅
    return str(value)


def value_to_notion_property(
    value: Any,
    prop_schema: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    단일 값 + 해당 프로퍼티 스키마 → Notion property payload 생성
    (지원하지 않는 타입은 None 리턴해서 건너뜀)
    """
    if _is_null(value):
        # 비워두고 싶으면 그냥 프로퍼티를 보내지 않는게 가장 안전
        return None

    ptype = prop_schema.get("type")

    # 가장 자주 쓰일만한 타입들만 구현
    if ptype == "title":
        return {
            "title": [
                {
                    "type": "text",
                    "text": {"content": str(value)},
                }
            ]
        }

    if ptype == "rich_text":
        return {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": str(value)},
                }
            ]
        }

    if ptype == "number":
        try:
            num = float(value)
        except Exception:
            # 숫자로 변환 실패하면 문자열로 rich_text로 보내고 싶다면 여기서 분기
            return None
        return {"number": num}

    if ptype == "checkbox":
        if isinstance(value, str):
            lowered = value.strip().lower()
            bool_val = lowered in ("true", "1", "y", "yes", "t")
        else:
            bool_val = bool(value)
        return {"checkbox": bool_val}

    if ptype == "select":
        return {
            "select": {
                "name": str(value),
            }
        }

    if ptype == "multi_select":
        # list 또는 콤마로 구분된 문자열 지원
        if isinstance(value, (list, tuple, set)):
            names = [str(v).strip() for v in value if not _is_null(v)]
        else:
            names = [v.strip() for v in str(value).split(",") if v.strip()]
        if not names:
            return None
        return {
            "multi_select": [{"name": n} for n in names]
        }

    if ptype == "date":
        return {
            "date": {
                "start": _to_iso_date(value),
            }
        }

    # 그 외 (people, relation, files, url, email, phone_number, status, formula, rollup 등)
    # 은 여기서 별도 구현하거나, User가 직접 확장하도록 남겨둔다.
    # 기본적으로는 스킵
    return None


def upload_dataframe_to_notion_data_source(
    df: pd.DataFrame,
    data_source_id: str,
    token: str,
    column_mapping: Optional[Dict[str, str]] = None,
) -> list[str]:
    """
    pandas DataFrame 을 Notion data source 로 전송하는 함수.

    Parameters
    ----------
    df : pd.DataFrame
        업로드할 데이터 프레임 (각 행 → Notion 한 row)
    data_source_id : str
        Notion data source ID (Notion 앱에서 "Copy data source ID" 로 복사한 값)
    token : str
        Notion Integration 의 시크릿 토큰 (e.g. "secret_xxx")
    column_mapping : dict, optional
        {DataFrame 컬럼명: Notion property 이름} 매핑.
        지정하지 않으면 컬럼명과 프로퍼티 이름이 동일하다고 가정.

    Returns
    -------
    list[str]
        생성된 Notion page id 리스트
    """
    if column_mapping is None:
        column_mapping = {col: col for col in df.columns}

    # 1) data source 스키마 가져오기
    properties_schema = get_data_source_schema(data_source_id, token)

    created_page_ids: list[str] = []
    headers = _make_headers(token)

    for idx, row in df.iterrows():
        properties_payload: Dict[str, Any] = {}

        for df_col, notion_prop_name in column_mapping.items():
            if notion_prop_name not in properties_schema:
                # 스키마에 없는 컬럼은 무시
                continue

            prop_schema = properties_schema[notion_prop_name]
            value = row.get(df_col)

            prop_payload = value_to_notion_property(value, prop_schema)
            if prop_payload is not None:
                properties_payload[notion_prop_name] = prop_payload

        if not properties_payload:
            # 전송할 프로퍼티가 없으면 row 스킵
            continue

        body = {
            "parent": {
                "type": "data_source_id",
                "data_source_id": data_source_id,
            },
            "properties": properties_payload,
        }

        res = requests.post(
            f"{BASE_URL}/pages",
            headers=headers,
            json=body,
        )

        if res.status_code == 429:
            raise NotionAPIError(
                "Rate limited by Notion API (HTTP 429). "
                "잠시 후 다시 시도하거나, 백오프 로직을 추가하세요."
            )

        if not res.ok:
            raise NotionAPIError(
                f"Failed to create page for row {idx}: "
                f"{res.status_code} {res.text}"
            )

        created_page_ids.append(res.json()["id"])

    return created_page_ids
