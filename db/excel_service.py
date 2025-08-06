from db import get_connection
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]

def save_excel_to_db():
    # 엑셀 불러오기 추후 수정해야함
    df = pd.read_excel(BASE_DIR / "excel" / "input" / "신용공여 잔고 추이 (7).xlsx", skiprows=3)

    # 열 이름 지정
    df.columns = [
        "date",
        "loan_total",
        "loan_kospi",
        "loan_kosdaq",
        "short_total",
        "short_kospi",
        "short_kosdaq",
        "subscription_loan",
        "collateral_loan"
    ]

    # date 형식 변환
    df["date"] = pd.to_datetime(df["date"], format="%Y/%m/%d")

    # 숫자형 변환
    numeric_cols =  df.columns[1:]
    for col in numeric_cols:
        df[col] = (
            df[col]
            .replace("-", pd.NA) # None, pd.Na 
            .str.replace(",", "", regex=False)
            .astype("Int64")
            .multiply(1_000_000) # multiply
        )
    

    # DB
    conn = get_connection()
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO credit_transactions (
            date, loan_total, loan_kospi, loan_kosdaq,
            short_total, short_kospi, short_kosdaq,
            subscription_loan, collateral_loan
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING;
    """

    for _, row in df.iterrows():
        row_clean = row.where(pd.notna(row), None)
        cur.execute(insert_sql, tuple(row_clean))


    conn.commit()
    cur.close()
    conn.close()
    print("DB 저장 완료")



def save_kospi_excel_to_db():
    df = pd.read_excel(BASE_DIR / "excel" / "input" / "유가증권시장 (6).xlsx", skiprows=2)
    
    # 속성명 변경
    df.columns = [
    "date",                # 구 분
    "kospi_index",         # KOSPI지수
    "volume",              # 거래량
    "trade_value",         # 거래대금
    "market_cap",          # 시가총액
    "foreign_market_cap",  # 외국인 시가총액
    "foreign_ratio"        # 외국인 비중
]

    #date 변경
    df['date'] = pd.to_datetime(df['date'], format="%Y/%m/%d")

    # 소수로 유지할 컬럼들 (ex: kospi_index)
    float_cols = ["kospi_index", "foreign_ratio"]

    # 나머지는 정수로 변환  
    numeric_cols = df.columns[1:]

    for col in numeric_cols:
        df[col] = (
            df[col]
            .astype(str)  # ✅ 먼저 문자열로 변환
            .replace("-", pd.NA)  # '-' → NA
            .str.replace(",", "", regex=False)  # 쉼표 제거
        )

    # 필요한 열은 float으로
    for col in float_cols:
        df[col] = df[col].astype(float)

    # 나머지 열은 정수로
    for col in numeric_cols:
        if col not in float_cols:
            df[col] = df[col].astype("Int64")


    # db
    conn = get_connection()
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO kospi_summary (
            date, kospi_index, volume, trade_value,
            market_cap, foreign_market_cap, foreign_ratio
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING;
    """

    for _, row in df.iterrows():
        cur.execute(insert_sql, tuple(row))

    conn.commit()
    cur.close()
    conn.close()
    print("DB 저장 완료")

save_excel_to_db()