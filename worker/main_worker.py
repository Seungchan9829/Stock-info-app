import os, asyncio
import logging
import traceback
import pytz
from datetime import datetime
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler  # 변경 포인트
from coin.main_coin_alert import main_coin_alert
from nasdaq_100 import nasdaq_100, SNP_500
from worker.alert_stock_info_by_discord import run_discord_bot
from worker.update_stock_info_by_yfinance import (
    stock_price_update_by_yfinance_run,
    stock_info_update_run
)
from kr_index import KOSPI_50, KOSDAQ_150

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,  # 필요시 DEBUG로 변경 가능
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("DISCORD_STOCK_CHANNEL"))

# 뉴욕 시간대
NY_TZ = pytz.timezone("America/New_York")
# 한국 시간대
KR_TZ = pytz.timezone("Asia/Seoul")

def job():
    now = datetime.now(NY_TZ)
    logger.info(f"[JOB] 실행: {now}")
    try:
        logger.info("나스닥 가격 업데이트 JOB 실행")
        stock_price_update_by_yfinance_run(nasdaq_100, "10d")
        logger.info("S&P500 가격 정보 업데이트 JOB 실행")
        stock_price_update_by_yfinance_run(SNP_500, "10d")

    except Exception:
        logger.error("[JOB] 예외 발생:\n%s", traceback.format_exc())

def korea_stock_update_job():
    now = datetime.now(KR_TZ)
    logger.info(f"한국 주식 업데이트 [JOB] 실행 : {now}")
    try:
        logger.info("코스피 50 가격 업데이트 JOB 실행")
        stock_price_update_by_yfinance_run(KOSPI_50, "10d")
        logger.info("코스닥 150 가격 업데이트 JOB 실행")
        stock_price_update_by_yfinance_run(KOSDAQ_150, "10d")
    except Exception:
        logger.error("한국 주식 업데이트 [JOB] 오류 발생")

async def main():
    logger.info("main_worker 실행")

    # 스케줄러: asyncio 루프에 붙임
    scheduler = AsyncIOScheduler(timezone=NY_TZ)
    scheduler.add_job(job, 'cron', day_of_week='mon-fri', hour=9, minute=30)
    scheduler.add_job(job, 'cron', day_of_week='mon-fri', hour='10-15', minute=30)

        # ── 한국장: 잡 트리거에 timezone=KR_TZ 지정 ──
    # 장 시작/정각 업데이트 (예: 09:00, 10:00~15:00)
    scheduler.add_job(
        korea_stock_update_job, 'cron',
        day_of_week='mon-fri', hour=9, minute=0, timezone=KR_TZ, id="kr_open", replace_existing=True
    )
    scheduler.add_job(
        korea_stock_update_job, 'cron',
        day_of_week='mon-fri', hour='10-15', minute=0, timezone=KR_TZ, id="kr_hours", replace_existing=True
    )
    # 장마감 시점 처리(15:30)
    scheduler.add_job(
        korea_stock_update_job, 'cron',
        day_of_week='mon-fri', hour=15, minute=30, timezone=KR_TZ, id="kr_close", replace_existing=True
    )


    scheduler.start()

    # 초기 실행(동기 함수면 스레드 풀로 넘기기)
    loop = asyncio.get_running_loop()

    logger.info("코스피 종목 정보 초기 업데이트")
    await loop.run_in_executor(None, stock_info_update_run, KOSPI_50)

    logger.info("나스닥 종목 정보 초기 업데이트")
    await loop.run_in_executor(None, stock_info_update_run, nasdaq_100)

    logger.info("코스닥 종목 정보 초기 업데이트")
    await loop.run_in_executor(None, stock_info_update_run, KOSDAQ_150)

    logger.info("S&P500 종목 정보 초기 업데이트")
    await loop.run_in_executor(None, stock_info_update_run, SNP_500)


    # 가격 정보 초기 업데이트
    logger.info("나스닥 가격 정보 초기 업데이트")
    await loop.run_in_executor(None, stock_price_update_by_yfinance_run, nasdaq_100, "10d")
    logger.info("코스피 가격 정보 초기 업데이트")
    await loop.run_in_executor(None, stock_price_update_by_yfinance_run, KOSPI_50, "2y")
    logger.info("코스닥 가격 정보 초기 업데이트")
    await loop.run_in_executor(None, stock_price_update_by_yfinance_run, KOSDAQ_150, "2y")
    logger.info("S&P500 가격 정보 초기 업데이트")
    await loop.run_in_executor(None, stock_price_update_by_yfinance_run, SNP_500, "2y")

    logger.info("디스코드 봇 & 코인 알람 동시 실행")
    await asyncio.gather(
        run_discord_bot(TOKEN, CHANNEL_ID),  # 디스코드 봇
        main_coin_alert(),                          # 코인 알람
    )

if __name__ == "__main__":
    asyncio.run(main())