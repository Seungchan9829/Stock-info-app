from fetchStockPrice import fetch_stock_price
from get_buy_points import compute_disparity
from get_buy_points import get_buy_points
from nasdaq_100 import nasdaq_100


# 나스닥 100개의 종목 -> (티커, DI20, MA20) 반환 함수
def filter_low_di20():
    res = []
    for ticker in nasdaq_100:
        df = fetch_stock_price(ticker, "1y")
        result = compute_disparity(df)

        # p7
        p7 = get_buy_points(result)
        print(df, result)
        yesterday_close = df[("Close",ticker)].iloc[-1]

        yesterday_ma20 = result["MA20"].iloc[-1]


        if result["DI20"].iloc[-1] <= p7:
            res.append((ticker, format(p7, ".2f"), format(result["DI20"].iloc[-1], ".2f")))
    
    return res


filter_low_di20()