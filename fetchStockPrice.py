# yFinance로 주가 데이터 받아와서 DataFrame 반환 함수
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime



CACHE_DIR = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)

def fetch_stock_price(ticker: str, period: str) -> pd.DataFrame:
    # 캐시 여부 확인
    cache_file = CACHE_DIR / f"{ticker}_{period}.pkl"

    if cache_file.exists():
        mtime = datetime.fromtimestamp(cache_file.stat().st_mtime).date()
        
        if mtime == datetime.now().date():
            print(f"📂 Loaded up-to-date cache ({mtime})")
            return pd.read_pickle(cache_file)
        else:
            print(f"🔄 Cache stale (last saved {mtime}), refreshing…")
    
    df = yf.download(tickers = ticker, period = period, auto_adjust=True, rounding=True)
    
    print(ticker, df)
    if not df.empty:
        df.to_pickle(cache_file)
    

    return df



