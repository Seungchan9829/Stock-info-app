# 예) 1년치 테슬라 일별 주가 데이터를 입력할 경우 각 월별로 가장 낮은 20일 이격도를 리스트에 저장
# 각 리스트를 정렬하여 하위 3개의 이격도를 사용
import pandas as pd

def compute_disparity(df : pd.DataFrame) -> pd.DataFrame:
    close = df['Close']

    ma20 = close.rolling(20).mean()

    di20 = (close - ma20) / ma20 * 100

    return df.assign(MA20 = ma20, DI20 = di20).dropna()
    

def get_buy_points(df : pd.DataFrame):

    p7 = df['DI20'].quantile(0.07)

    return p7

    