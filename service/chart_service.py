from db.chart_repository import get_credit_finance_data, get_kospi_price_data
from datetime import datetime, timedelta
from tools.cal_ma20_di20 import cal_ma20_di20
import pandas as pd
def get_credit_finance_data_by_period(period : str) -> list:
    # 기간 설정
    today = datetime.today()

    if period == "1y":
        start = today - timedelta(days=365)
    elif period == "3y":
        start = today - timedelta(days=3 * 365)
    elif period == "5y":
        start = today - timedelta(days=5 * 365)
    else:
        raise ValueError("Invalid period")


    raw_data = get_credit_finance_data(start, today)

    return raw_data


def get_kospi_price_data_by_period(period : str) -> list:
    # 기간 설정
    today = datetime.today()

    if period == "1y":
        start = today - timedelta(days=365)
    elif period == "3y":
        start = today - timedelta(days=3 * 365)
    elif period == "5y":
        start = today - timedelta(days=5 * 365)
    else:
        raise ValueError("Invalid period")
    
    raw_data = get_kospi_price_data(start,today)
    return raw_data

def get_credit_data_di20(period : str):
    # 기간 설정
    today = datetime.today()

    if period == "1y":
        start = today - timedelta(days=365 + 60)
    elif period == "3y":
        start = today - timedelta(days=3 * 365 + 60)
    elif period == "5y":
        start = today - timedelta(days=5 * 365 + 60)
    else:
        raise ValueError("Invalid period")
    
    # 신용 잔고 데이터 list[tuple]
    credit_raw_data = get_credit_finance_data(start, today)

    credit_df = pd.DataFrame(credit_raw_data, columns = ("date", "loan_total", "loan_kospi", "loan_kosdaq"))

    cal_credit_df = cal_ma20_di20(credit_df.sort_values(by="date", ascending=True))

    credit_date_di20 = cal_credit_df[['date', 'DI20']]

    # 특정 date 설정. 기존에 더 많은 일자 데이터를 가지고 왔기 때문
    specific_date = pd.to_datetime(today - timedelta(days=365))
    credit_date_di20["date"] = pd.to_datetime(credit_date_di20["date"])

    credit_date_di20 = credit_date_di20[
    credit_date_di20["date"] >= specific_date
]


    p7 = credit_date_di20['DI20'].quantile(0.07)


    return (credit_date_di20, p7)


get_credit_data_di20("1y")