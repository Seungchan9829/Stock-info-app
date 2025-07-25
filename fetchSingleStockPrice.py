import pandas as pd
import yfinance as yf
from get_buy_points import compute_disparity

def fetch_single_stock_price(ticker : str, period : str) -> pd.DataFrame:

    df = yf.download(tickers = ticker, period = period, auto_adjust=True, rounding=True)

    print(df)

    result = compute_disparity(df)

    print(result)



fetch_single_stock_price("ZS", "1y")