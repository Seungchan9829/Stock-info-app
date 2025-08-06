from nasdaq_100 import nasdaq_100
from db.db import get_connection
from datetime import date
from decimal import Decimal
import pandas as pd

def get_stock_price_by_days(ticker : str, days : int) -> list[tuple[date, Decimal]]:
    conn = get_connection()

    sql = """
        SELECT sp.date, sp.close
        FROM stock_info si
        JOIN stock_prices sp ON si.id = sp.stock_id
        WHERE si.ticker = %s
        ORDER BY date DESC
        LIMIT %s
    """

    with conn.cursor() as cur:
        cur.execute(sql, (ticker, days))
        result = cur.fetchall()

    conn.close()    
    return result


# 오늘의 과대 낙폭 종목 리스트를 반환하는 함수
def get_today_low_di20_stocks() -> list[tuple[str, float,float]]:
    # 결과 값을 담을 리스트
    low_di20_stocks = []

    # 종목 리스트
    stock_list = nasdaq_100

    for ticker in stock_list:
        # DB 가격 데이터 가져오기
        stock_price_list = get_stock_price_by_days(ticker, 252)

        # 데이터 가공
        df = (
            pd.DataFrame(stock_price_list, columns = ['date', 'close'])
            .astype({'close': float})
            .sort_values('date')
        )
        df['ma20'] = df['close'].rolling(20).mean()
        df['di20'] = (df['close'] - df['ma20']) / df['ma20'] * 100

        p = df['di20'].quantile(0.07)

        if df['di20'].iloc[-1] <= p:
            low_di20_stocks.append((ticker, df['di20'].iloc[-1], p))
    
    return low_di20_stocks




if __name__ == "__main__":
    get_today_low_di20_stocks()