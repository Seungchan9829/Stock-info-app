# 특정 일에 이격도 지표에 해당하는 종목들 일자별 파일시스템에 저장 예) 2025_05_25_low_di20_stock.pkl
from pathlib import Path
import pandas as pd
from get_buy_points import compute_disparity
from get_buy_points import get_buy_points

CACHE_DIR = Path(__file__).parents[1] / "cache"
LOW_DI20_DIR = Path(__file__).parents[1] / "low_di20"

# 티커와 날짜를 매개변수로 하여 해당 일에 적용되는 종목(티커, 기준이격도, 현재이격도) 리스트로 반환 함수
def get_low_di20_by_day(tickers : list, date : str):
    stock_list = []
    ts = pd.to_datetime(date) # 문자열 -> timestamp
    low_di20_file = LOW_DI20_DIR / f"{date}_stock_list.pkl"

    for ticker in tickers:
        # 해당 날짜의 주가 데이터가 캐시 확인
        cache_file = CACHE_DIR / f"{ticker}_1y.pkl"

        if not cache_file.exists():
            print("캐시(데이터) 파일이 없습니다.")
            continue
        
        stock_df = pd.read_pickle(cache_file)   
        computed_stock_df = compute_disparity(stock_df)
        p7 = get_buy_points(computed_stock_df)



        if ts not in computed_stock_df.index:
            continue
            
        DI20 = computed_stock_df.loc[ts, ("DI20", "")]
        if DI20 <= p7:
            stock_list.append((ticker, format(p7, ".2f"), format(DI20, ".2f")))
                


    pd.to_pickle(stock_list, low_di20_file)
    print(f"{date} 일자에 해당하는 주식 종목 저장 완료")
    print(stock_list)
