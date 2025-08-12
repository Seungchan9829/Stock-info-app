import os
import logging
import traceback
import pytz
from datetime import datetime
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler

from worker.alert_stock_info_by_discord import run_discord_bot
from worker.update_stock_info_by_yfinance import (
    stock_info_update_by_yfinance_run,
    stock_price_update_by_yfinance_run
)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,  # 필요시 DEBUG로 변경 가능
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = 1400914107233730622

# 뉴욕 시간대
NY_TZ = pytz.timezone("America/New_York")

def job():
    now = datetime.now(NY_TZ)
    logger.info(f"[JOB] 실행: {now}")
    try:
        logger.info("stock_price_update_by_yfinance_run() 호출")
        stock_price_update_by_yfinance_run()
    except Exception:
        logger.error("[JOB] 예외 발생:\n%s", traceback.format_exc())

if __name__ == "__main__":
    logger.info("main_worker 실행")
    logger.info("스케줄러 시작(백그라운드)")

    scheduler = BackgroundScheduler(timezone=NY_TZ)
    scheduler.add_job(job, 'cron', day_of_week='mon-fri', hour=9, minute=30)
    scheduler.add_job(job, 'cron', day_of_week='mon-fri', hour='10-15', minute=30)
    scheduler.start()

    logger.info("stock_info_update_by_yfinance_run() 호출 직전")
    stock_info_update_by_yfinance_run()

    logger.info("stock_price_update_by_yfinance_run() 호출 직전")
    stock_price_update_by_yfinance_run()

    logger.info("디스코드 봇 실행 직전")
    run_discord_bot(TOKEN, CHANNEL_ID)
