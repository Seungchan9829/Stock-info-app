# 한번씩 주식 정보 리프레시 하는 함수
from pathlib import Path
import yfinance as yf
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from tools.get_low_di20_by_day import get_low_di20_by_day
from nasdaq_100 import nasdaq_100

# 캐시 디렉토리
CACHE_DIR = Path(__file__).parents[1] / "cache"


# 1년간의 주가 데이터를 갱신 하는 함수
def refresh_stock_info(tickers: list):
    for ticker in tickers:
        cache_file = CACHE_DIR / f"{ticker}_1y.pkl"

        df = yf.download(tickers = ticker, period = "1y", auto_adjust=True, rounding=True)
        
        if not df.empty:
            df.to_pickle(cache_file)
        
    
    print("종목 정보 갱신완료")
    
    return None


# 최근 30일까지 이격도 해당 종목 갱신하는 함수
def refresh_low_di20_stock_list():
    today = date.today()
    start = (today - relativedelta(months=1)).replace(day=20)
    end = today

    for i in range((end - start).days + 1):
        current = start + timedelta(days=i)
        date_str = current.strftime("%Y-%m-%d")
        get_low_di20_by_day(nasdaq_100, date_str)
    print("30일 이격도 갱신 완료")

    return None



refresh_low_di20_stock_list()


