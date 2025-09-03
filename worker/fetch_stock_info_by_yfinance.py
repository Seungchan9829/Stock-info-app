import yfinance as yf
import time, logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from nasdaq_100 import nasdaq_100
from typing import Optional, Tuple, List

logging.basicConfig(
    level=logging.INFO,  # 필요시 DEBUG로 변경 가능
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
def _safe_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None
    
def fetch_stock_info_one(ticker: str) -> Tuple[str, Optional[str], Optional[str], Optional[str], Optional[int]]:
    t = yf.Ticker(ticker)

    # 1) get_info: 이름/국가
    info = {}
    try:
        info = t.get_info() or {}
    except Exception:
        pass
    fullname = info.get("longName") or info.get("shortName") or info.get("displayName")
    country  = info.get("country")    # 예: "United States" (없을 수 있음)

    # 2) 거래소: metadata 우선, 없으면 info에서 대체
    exchange = None
    try:
        meta = t.get_history_metadata() or {}
        exchange = meta.get("exchangeName") or meta.get("fullExchangeName")
    except Exception:
        pass
    if not exchange:
        exchange = info.get("fullExchangeName") or info.get("exchange")  # 예: "NasdaqGS" / "NMS"

    # 3) 시가총액: fast_info 우선 → info → 종가×발행주식수
    marketcap = None
    try:
        marketcap = _safe_int(t.fast_info["market_cap"])
    except Exception:
        pass
    if marketcap is None:
        marketcap = _safe_int(info.get("marketCap"))

    if marketcap is None:
        # 종가 × 발행주식수로 보강
        try:
            last_close = float(t.history(period="1d", auto_adjust=False)["Close"].iloc[-1])
            shares = int(t.get_shares_full().iloc[-1])
            marketcap = _safe_int(last_close * shares)
        except Exception:
            pass

    return (ticker, fullname, exchange, country, marketcap)

def fetch_many_stock_info(tickers: List[str], max_workers: int = 8, batch_size: int = 50):
    """배치 사이즈만 도입: 티커를 batch_size로 쪼개 순차 처리"""
    rows: List[Tuple[str, Optional[str], Optional[str], Optional[str], Optional[int]]] = []
    n = len(tickers)
    logging.info("yfinance 종목 정보 가져오기 시작 (총 %d)", n)

    for start in range(0, n, batch_size):
        batch = tickers[start:start + batch_size]
        logging.info("배치 처리: %d~%d (크기 %d)", start + 1, start + len(batch), len(batch))

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(fetch_stock_info_one, t) for t in batch]
            for fut in as_completed(futures):
                try:
                    rows.append(fut.result())
                except Exception as e:
                    # 개별 실패는 건너뛰고 계속
                    logging.warning("개별 요청 실패: %s", e)

    logging.info("주식 정보 가져오기 끝 (수집 %d)", len(rows))
    return rows


if __name__ == "__main__":
    t = yf.Ticker("005930.KS")

    info = t.get_info()
    fullname = info.get("longName") or info.get("shortName") or info.get("displayName")

    print(fullname)