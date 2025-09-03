# 함수의 목적 : 일정 시간마다 실행되어 yfinance를 통해서 주식 종목의 정보를 갱신한다.
from nasdaq_100 import nasdaq_100
from db.db import get_connection
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf
import logging, time
from psycopg2.extras import execute_values
from worker.fetch_stock_info_by_yfinance import fetch_many_stock_info
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,  # 필요시 DEBUG로 변경 가능
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

def get_stock_ids(tickers: list[str]) -> dict[str, int]:
    if not tickers:
        return {}
    conn = get_connection()
    try:
        conn.set_session(readonly=True, autocommit=True)  # SELECT 전용일 때
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, ticker
                FROM stock_info
                WHERE ticker = ANY(%s::text[])
            """, (tickers,))
            return {t: i for i, t in cur.fetchall()}
    finally:
        conn.close()
        
def save_price_to_db(rows: list[tuple], page_size: int = 1000) -> int:
    """
    rows: [(stock_id, date, open, high, low, close, volume), ...]
    return: 반영(INSERT+UPDATE)된 행 수
    """
    if not rows:
        logging.info("저장할 행이 없음. DB 작업 생략")
        return 0

    # (stock_id, date) UNIQUE 또는 PK 인덱스가 있어야 합니다.
    sql = """
        INSERT INTO stock_prices (stock_id, date, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (stock_id, date) DO UPDATE
        SET open   = EXCLUDED.open,
            high   = EXCLUDED.high,
            low    = EXCLUDED.low,
            close  = EXCLUDED.close,
            volume = EXCLUDED.volume
        -- 값이 바뀔 때만 UPDATE (WAL/인덱스 부하 감소)
        WHERE (stock_prices.open, stock_prices.high, stock_prices.low, stock_prices.close, stock_prices.volume)
           IS DISTINCT FROM
              (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close, EXCLUDED.volume)
        RETURNING 1
    """

    conn = None
    try:
        conn = get_connection()
        with conn:  # 정상 종료 시 commit, 예외 시 rollback
            with conn.cursor() as cur:
                # template을 명시하면 가독성/안전성↑
                changed = execute_values(
                    cur, sql, rows, page_size=page_size,
                    template="(%s,%s,%s,%s,%s,%s,%s)",
                    fetch=True  # RETURNING 결과 받기
                )
                affected = len(changed)  # 이번 쿼리로 실제 반영(INSERT+UPDATE)된 행 수
        logging.info("가격 데이터 저장 완료: %d행 반영", affected)
        return affected
    except Exception:
        logging.exception("DB 저장 중 예외 발생")
        return 0
    finally:
        if conn:
            conn.close()


def stock_info_update_run(tickers : list):
    # 매개변수 검증
    if not tickers:
        logger.info("티커 리스트 비어있음.")
        return
    
    # 티커에 해당하는 종목들 가져오기 병렬로
    stock_info_list = fetch_many_stock_info(tickers=tickers, max_workers=8)
    if not stock_info_list:
        logger.info("수집된 행이 없음. DB 작업 생략")
        return
    
    conn = None
    try:
        conn = get_connection()
        with conn:
            with conn.cursor() as cur:
                insert_sql = """
                    INSERT INTO stock_info (ticker, fullname, exchange, country, marketcap)
                    VALUES %s
                    ON CONFLICT (ticker) DO UPDATE
                    SET fullname  = EXCLUDED.fullname,
                        exchange  = EXCLUDED.exchange,
                        country   = EXCLUDED.country,
                        marketcap = EXCLUDED.marketcap
                    WHERE (stock_info.fullname, stock_info.exchange, stock_info.country, stock_info.marketcap)
                    IS DISTINCT FROM
                        (EXCLUDED.fullname,  EXCLUDED.exchange,  EXCLUDED.country,  EXCLUDED.marketcap)
                    """
                execute_values(cur, insert_sql, stock_info_list, page_size=1000)
    except Exception as e:
        logging.exception("DB 저장 중 예외 발생")
    finally:
        if conn:
            conn.close()
        logging.info("DB 저장 완료")

def stock_price_update_by_yfinance_run(tickers: list, period: str = "10d",
                                       batch_size: int = 50):
    if not tickers:
        logger.info("티커 리스트 비어있음")
        return

    stock_id_map = get_stock_ids(tickers)  # {ticker: id}

    total_inserted = 0
    n = len(tickers)
    for start in range(0, n, batch_size):
        batch = tickers[start:start+batch_size]
        logger.info("배치 %d~%d (%d개) 다운로드 시작",
                    start+1, start+len(batch), len(batch))

        # 한 번에 여러 종목 다운로드
        # threads=False: yfinance 내부 스레딩 비활성(외부에서 동시성 쓰지 않는다면 안전)
        df = yf.download(
            tickers=batch, period=period,
            auto_adjust=True, rounding=True, progress=False,
            group_by="ticker", threads=False
        )

        if df is None or df.empty:
            logger.info("배치 결과 없음(빈 DF). 건너뜀")
            continue

        rows_batch = []

        # 멀티-티커일 때: 컬럼이 MultiIndex (level0=ticker, level1=OHLCV)
        if isinstance(df.columns, pd.MultiIndex) and df.columns.nlevels == 2:
            # 현재 DF에 실제로 존재하는 티커만 추출
            present = list(pd.unique(df.columns.get_level_values(0)))
            for t in present:
                sid = stock_id_map.get(t)
                if sid is None:
                    logger.warning("DB에 없는 티커(건너뜀): %s", t)
                    continue

                sub = df[t].reset_index()  # Date + OHLCV
                # 날짜 컬럼 이름 통일
                dt_cols = [c for c in sub.columns if is_datetime(sub[c])]
                dt_col = dt_cols[0] if dt_cols else sub.columns[0]
                if dt_col != "Date":
                    sub = sub.rename(columns={dt_col: "Date"})

                for rec in sub.itertuples(index=False, name=None):
                    # rec: (Date, Open, High, Low, Close, Volume) 순서 보장 전 처리
                    # 필요한 컬럼 순서 맞추기
                    cols = sub.columns.tolist()
                    # 안전하게 가져오기
                    dt    = rec[cols.index("Date")]   if "Date"   in cols else None
                    open_ = rec[cols.index("Open")]   if "Open"   in cols else None
                    high_ = rec[cols.index("High")]   if "High"   in cols else None
                    low_  = rec[cols.index("Low")]    if "Low"    in cols else None
                    close_= rec[cols.index("Close")]  if "Close"  in cols else None
                    vol   = rec[cols.index("Volume")] if "Volume" in cols else None

                    # 타임존 제거
                    if isinstance(dt, pd.Timestamp) and dt.tz is not None:
                        try:
                            dt = dt.tz_localize(None)
                        except Exception:
                            pass
                    # 볼륨 정수화
                    if pd.isna(vol):
                        vol_out = None
                    else:
                        try: vol_out = int(vol)
                        except Exception: vol_out = None

                    rows_batch.append((sid, dt, open_, high_, low_, close_, vol_out))

        else:
            # 단일 티커만 반환된 경우(컬럼 단일 레벨)
            # yfinance가 실패로 일부만 내려주면 이런 케이스가 나올 수 있음
            t = batch[0]
            sid = stock_id_map.get(t)
            if sid is None:
                logger.warning("DB에 없는 티커(건너뜀): %s", t)
            else:
                sub = df.reset_index()
                dt_cols = [c for c in sub.columns if is_datetime(sub[c])]
                dt_col = dt_cols[0] if dt_cols else sub.columns[0]
                if dt_col != "Date":
                    sub = sub.rename(columns={dt_col: "Date"})

                for rec in sub.itertuples(index=False, name=None):
                    cols = sub.columns.tolist()
                    dt    = rec[cols.index("Date")]   if "Date"   in cols else None
                    open_ = rec[cols.index("Open")]   if "Open"   in cols else None
                    high_ = rec[cols.index("High")]   if "High"   in cols else None
                    low_  = rec[cols.index("Low")]    if "Low"    in cols else None
                    close_= rec[cols.index("Close")]  if "Close"  in cols else None
                    vol   = rec[cols.index("Volume")] if "Volume" in cols else None

                    if isinstance(dt, pd.Timestamp) and dt.tz is not None:
                        try: dt = dt.tz_localize(None)
                        except Exception: pass
                    if pd.isna(vol):
                        vol_out = None
                    else:
                        try: vol_out = int(vol)
                        except Exception: vol_out = None

                    rows_batch.append((sid, dt, open_, high_, low_, close_, vol_out))

        if not rows_batch:
            logger.info("배치 변환 결과 0행. 건너뜀")
            continue

        # 배치 단위로 DB 저장 (execute_values 사용 권장)
        inserted = save_price_to_db(rows_batch)  # 반환값을 반영 행 수로 만들었다면 집계 가능
        total_inserted += inserted if inserted else 0
        logger.info("배치 저장 완료: %d행 (누적 %d)", len(rows_batch), total_inserted)

    logger.info("모든 가격 데이터 저장 종료. 누적 반영 %d", total_inserted)