import pandas as pd

def preprocess_reservation(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.rename(columns={"No.": "차트번호"}).iloc[:-1]
    df["예약일시"] = df.예약일 + " " + df.시간 + ":00.000+09:00"
    df["등록일"] = df["등록일시"].str[:10]
    df["차트번호"] = df["차트번호"].astype(int)
    df = df.loc[:,["등록일", "예약일시", "차트번호", "고객명", "구분", "상태", "수술/시술", "상담자", "원장", "국가", "고객1차경로", "고객2차경로", "메모", "특이사항"]]
    
    return df

def preprocess_sales(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.rename(columns={"No.": "차트번호"}).iloc[:-1]
    df["차트번호"] = df["차트번호"].astype(int)
    df = df.loc[:,["정산일", "차트번호", "고객명", "상태", "최종상태", "상담자", "국가", "1차경로", "2차경로", "3차경로", "지점", "수술/시술비"]]
    
    return df