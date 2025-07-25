import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt
import matplotlib as mpl


# 1) 한글 폰트 설정 (macOS 예시: AppleGothic)
mpl.rcParams['font.family'] = 'AppleGothic'

# Windows에서는 'Malgun Gothic', Linux에서는 'NanumGothic' 등을 사용하세요
# mpl.rcParams['font.family'] = 'Malgun Gothic'
# mpl.rcParams['font.family'] = 'NanumGothic'

# 2) 음수 부호(−)가 깨지는 경우 방지
mpl.rcParams['axes.unicode_minus'] = False

def fetch_data(ticker: str, years: int = 3) -> pd.DataFrame:
    period = f"{years}y" 
    df = yf.download(ticker, period=period, auto_adjust=True)
    # download -> pandas.dataFrame 반환

    # MultiIndex :
    # MultiIndex 평탄화
    if isinstance(df.columns, pd.MultiIndex):
        for lvl in range(df.columns.nlevels):
            vals = df.columns.get_level_values(lvl)
            if set(vals) == {ticker}:
                df.columns = df.columns.droplevel(lvl)
                break

    # 'Adj Close'→'Close' 리네임
    if 'Close' not in df.columns and 'Adj Close' in df.columns:
        df.rename(columns={'Adj Close': 'Close'}, inplace=True)

    df.dropna(inplace=True)
    return df

def compute_deviation(df: pd.DataFrame) -> pd.DataFrame:
    close = df['Close']
    ma20  = close.rolling(20).mean()
    dev20 = (close - ma20) / ma20 * 100
    return df.assign(MA20=ma20, Dev20=dev20).dropna()

def collect_all_buy_points(df: pd.DataFrame, window: int = 252) -> pd.DatetimeIndex:
    """
    • 월별 실제 마지막 거래일(ME 그룹)만 뽑아서
    • 각 날짜 이전 window일 간 Dev20 하위 3개(음수폭 큰) → 0에 가까운 순으로 정렬
    • 전 기간을 모두 수집, 중복 제거 후 정렬 반환
    """
    # 1) 월별 실제 마지막 거래일 리스트
    month_ends = (
        df
        .groupby(pd.Grouper(freq='ME'))       # Month End frequency
        .apply(lambda x: x.index[-1])         # 그룹별 실제 마지막 인덱스(거래일)
        .tolist()
    )
    # 2) 윈도우가 완성된 시점(인덱스 위치) 이후만
    month_ends = [d for d in month_ends if df.index.get_loc(d) >= window]

    all_dates = []
    for me in month_ends:
        window_slice = df['Dev20'].loc[:me].iloc[-window:]  # 마지막 me까지의 window일치 Dev20
        bottoms = window_slice.nsmallest(3)                 # 값이 작은(음수폭 큰) 3개
        ordered = bottoms.sort_values(ascending=False)      # 0에 가까운 순으로
        all_dates += list(ordered.index)

    # 중복 제거, 오름차순 정렬
    return pd.DatetimeIndex(sorted(set(all_dates)))

def simulate_strategy_equity(df: pd.DataFrame,
                             buy_dates: pd.DatetimeIndex,
                             target_gain: float = 0.10) -> pd.Series:
    equity = pd.Series(index=df.index, dtype=float)
    cash   = 1.0
    in_trade = False
    exit_map = {}

    # 각 매수일별 청산일 계산
    for bd in buy_dates:
        bp   = df.at[bd, 'Close']
        mask = df.loc[bd:].Close >= bp * (1 + target_gain)
        exit_map[bd] = mask.idxmax() if mask.any() else df.index[-1]

    # 일별 equity 계산
    for date in df.index:
        if (not in_trade) and (date in buy_dates):
            in_trade    = True
            entry_date  = date
            entry_price = df.at[date, 'Close']

        if in_trade:
            equity[date] = cash * df.at[date, 'Close'] / entry_price
            if date == exit_map[entry_date]:
                cash     = equity[date]
                in_trade = False
        else:
            equity[date] = cash

    return equity

if __name__ == "__main__":
    ticker = "TSLL"
    df     = fetch_data(ticker, years=3)
    df     = compute_deviation(df)

    # 전체 기간의 매수일 수집
    buy_dates = collect_all_buy_points(df, window=252)

    # — Dev20 차트 (매수 포인트 표시)
    plt.figure()
    plt.plot(df.index, df['Dev20'], linewidth=0.8, label='Dev20')
    plt.scatter(buy_dates, df.loc[buy_dates, 'Dev20'],
                color='red', s=30, label='매수 포인트')
    plt.axhline(0, linestyle='--', color='gray')
    plt.title(f"{ticker} 20일 이격도 & 매수 포인트")
    plt.xlabel("날짜")
    plt.ylabel("이격도 (%)")
    plt.legend()
    plt.tight_layout()
    plt.show()

    # — 전략 vs Buy & Hold 누적 수익률
    strat_eq = simulate_strategy_equity(df, buy_dates, target_gain=0.10)
    bh_eq    = df['Close'] / df['Close'].iloc[0]

    plt.figure()
    plt.plot(
    strat_eq.index, strat_eq,
    marker='o',
    linewidth=0.8,        # ← 여기서 선 두께를 지정 (기본 1.5)
    label='20일 이격도 전략'
)
    plt.plot(
    bh_eq.index, bh_eq,
    linewidth=0.8,        # Buy & Hold 선도 얇게
    label='Buy & Hold'
)
    plt.title(f"{ticker} 전략 vs Buy & Hold 누적 수익률 비교")
    plt.xlabel("날짜")
    plt.ylabel("누적 수익률 (배)")
    plt.legend()
    plt.tight_layout()
    plt.show()
