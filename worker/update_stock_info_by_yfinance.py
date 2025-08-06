# 함수의 목적 : 일정 시간마다 실행되어 yfinance를 통해서 주식 종목의 정보를 갱신한다.
from nasdaq_100 import nasdaq_100
from db.db import get_connection
import yfinance as yf

def get_stock_ids(tickers : list) -> dict[str,int]:
    conn = get_connection()

    insert_sql = """
        SELECT id, ticker from stock_info WHERE ticker = ANY(%s)
    """
    try:
        with conn.cursor() as cur:
            cur.execute(insert_sql, (tickers,))
            rows = cur.fetchall()
            return { ticker : id for id, ticker in rows}
    finally:
        conn.close()
        
def save_price_to_db(rows : list):
    conn = get_connection()   

    sql = """
        INSERT INTO stock_prices (stock_id, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_id, date) DO UPDATE
        SET open=EXCLUDED.open,
            high=EXCLUDED.high,
            low=EXCLUDED.low,
            close=EXCLUDED.close,
            volume=EXCLUDED.volume
    """
    
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    
    conn.commit()
    conn.close()

def stock_info_update_by_yfinance_run(tickers : list = nasdaq_100):
    # 1. DB 연결
    conn = get_connection()
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO stock_info (ticker, fullname, exchange, country, marketcap)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (ticker)
        DO UPDATE SET
            fullname=EXCLUDED.fullname,
            exchange=EXCLUDED.exchange,
            country=EXCLUDED.country,
            marketcap=EXCLUDED.marketcap
    """

    # 2. 종목별 데이터 가져오기
    for ticker in tickers:
        try:
            tick_info = yf.Ticker(ticker).info
            
            long_name, exchange, country, marketcap = (
                tick_info["longName"],
                tick_info["exchange"],
                tick_info["country"],
                tick_info["marketCap"]
            )       

            # 3. DB 저장
            cur.execute(insert_sql, (ticker, long_name, exchange, country, marketcap))
            print(f"{ticker} 저장 완료")

        except Exception as e:
            print(f"{ticker} 저장 실패:", e)
            
    # 4. DB 종료
    conn.commit()
    cur.close()
    conn.close()
    print("DB 저장 완료")

        


def stock_price_update_by_yfinance_run(tickers : list = nasdaq_100, period : str = "5d"):
    # 1. 업데이트할 티커(종목코드) 리스트 설정
    stocks_to_update = tickers
    
    # DB에서 티커 id 가져오기
    stock_id_map = get_stock_ids(tickers)
    # 2. 종목별 데이터 가져오기
    for ticker in stocks_to_update:
        stock_price = yf.download(tickers = ticker, period = period, rounding=True, auto_adjust=True)

        # 3. 데이터 가공
        flat_df = stock_price.stack(level = "Ticker", future_stack = True).reset_index()

        rows = [
            (
                stock_id_map[row['Ticker']],
                row['Date'],
                row['Open'],
                row['High'],
                row['Low'],
                row['Close'],
                row['Volume']
            )
            for _, row in flat_df.iterrows()
        ]
        # 3. DB에 티커 저장
        save_price_to_db(rows)
        print(f"{ticker} 가격 데이터 저장 완료")

 
if __name__ == "__main__":
    stock_info_update_by_yfinance_run(nasdaq_100)
    stock_price_update_by_yfinance_run(nasdaq_100, "5d")

    