"""Streamlit app to convert uploaded Excel files into downloadable CSV files."""
from __future__ import annotations

import io
from typing import Optional
import math

import pandas as pd
import streamlit as st

from df2notoin import upload_dataframe_to_notion_data_source 
from notion2df import load_notion_df_filtered
from preprocess import preprocess_reservation, preprocess_sales

def load_notion_by_chart_numbers(database_id, token, chart_numbers, batch_size=100):
    all_dfs = []
    chart_list = list(chart_numbers)
    total_batches = math.ceil(len(chart_list) / batch_size)

    for i in range(total_batches):
        batch = [int(x) for x in chart_list[i * batch_size : (i + 1) * batch_size]]

        filter_payload = {
            "or": [
                {"property": "차트번호", "number": {"equals": chart_no}}
                for chart_no in batch
            ]
        }

        df_batch = load_notion_df_filtered(database_id, token, filter_payload)
        all_dfs.append(df_batch)
        print(f"Batch {i+1}/{total_batches} loaded: {len(df_batch)} rows")

    return pd.concat(all_dfs, ignore_index=True).reset_index(drop=True)

def read_excel(file) -> Optional[pd.DataFrame]:
    """Read the uploaded Excel file into a DataFrame with basic error handling."""
    try:
        return pd.read_excel(file)
    except Exception as exc:  # pragma: no cover - user facing
        st.error(f"엑셀 파일을 읽는 중 오류가 발생했습니다: {exc}")
        return None

def convert_to_csv(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to CSV bytes without the index column."""
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue().encode("utf-8")

def st_excel_to_notion(key = None, fn_preprocess = preprocess_reservation, data_source_id_main = None,  data_source_id_sub = None):
    st.title("📁 Plasys 데이터 노션 업로드")
    st.write("Plasys에서 데이터를 다운받아 엑셀 파일을 업로드하세요.")

    uploaded_file = st.file_uploader("엑셀 파일을 업로드하세요.", type=["xls", "xlsx"], key = f"uploader_{key}")
    if uploaded_file is None:
        st.info(".xls 또는 .xlsx 파일을 선택해주세요.")
        return

    df = read_excel(uploaded_file)
    if df is None:
        return

    st.subheader("업로드한 데이터 미리보기")
    st.dataframe(df.head())

    st.subheader("업로드 예정 데이터")
    processed_df = fn_preprocess(df.copy())

    if key == "2":

        chart_numbers = processed_df["차트번호"].dropna().astype(int).unique()

        df_reservation = load_notion_by_chart_numbers(
            str(data_source_id_sub),
            NOTION_TOKEN,
            chart_numbers,
            batch_size=100
        )

        df_reservation = df_reservation[
             df_reservation["상태"].isin(["완료", "결정", "불가", "외출"])
             ]
        df_reservation["예약일시"] = pd.to_datetime(df_reservation["예약일시"], errors="coerce")

        idx = df_reservation.groupby("차트번호")["예약일시"].idxmax()
        df_reservation_ = df_reservation.loc[idx, ["차트번호", "page_id"]].rename(columns={"page_id": "예약리스트"})

        processed_df = pd.merge(
            processed_df,
            df_reservation_,
            how = "left",
            on = ["차트번호"]
            )

    st.dataframe(processed_df.head())

    csv_bytes = convert_to_csv(processed_df)

    st.download_button(
        label="CSV로 다운로드",
        data=csv_bytes,
        file_name="converted.csv",
        mime="text/csv",
        type="primary",
        key = f"download_{key}")

    st.subheader("Notion 업로드")
    st.info(f"전체 {len(processed_df)}개의 데이터가 Notion에 업로드 예정입니다.")
    if st.button("Notion에 업로드", key=f"btn_upload_{key}", type="primary"):

        st.info(f"전체 {len(processed_df)}개의 데이터가 Notion에 업로드 중 입니다.")
        page_ids = upload_dataframe_to_notion_data_source(
            processed_df,
            data_source_id = str(data_source_id_main),
            token = NOTION_TOKEN
        )
        messege = f"전체 {len(processed_df)}개의 데이터가 Notion에 {len(page_ids)}개의 페이지로 업로드되었습니다."
        st.success(messege)

NOTION_TOKEN = st.secrets["NOTION_TOKEN"]
DATA_SOURCE_ID_1 = st.secrets["DATA_SOURCE_ID_1"]
DATA_SOURCE_ID_2 = st.secrets["DATA_SOURCE_ID_2"]

def main() -> None:

    tab1, tab2 = st.tabs(["예약", "정산"])

    with tab1:
        st_excel_to_notion("1", preprocess_reservation, DATA_SOURCE_ID_1)

    with tab2:
        st_excel_to_notion("2", preprocess_sales, DATA_SOURCE_ID_2, DATA_SOURCE_ID_1)

if __name__ == "__main__":
    main()
