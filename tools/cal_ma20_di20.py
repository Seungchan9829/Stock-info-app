# DataFrame (Date, price) 형태 -> DataFrame(Date, price, ma20, di20) 반환
import pandas as pd

def cal_ma20_di20(df : pd.DataFrame) -> pd.DataFrame:
    
    df["MA20"] = df.iloc[:, 1].rolling(window=20).mean()

    df["DI20"] = ((df.iloc[:, 1] - df['MA20']) / df['MA20'] * 100).round(2)

    df = df.dropna(subset=["DI20"])
    
    return df