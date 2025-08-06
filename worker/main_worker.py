from worker.alert_stock_info_by_discord import run_discord_bot
from worker.update_stock_info_by_yfinance import stock_info_update_by_yfinance_run, stock_price_update_by_yfinance_run
from dotenv import load_dotenv
import os

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = 1400914107233730622

if __name__ == "__main__":


    print("main_worker 실행")
    print("stock_info_update_by_yfinance_run() 호출 직전")
    stock_info_update_by_yfinance_run()
    print("stock_price_update_by_yfinance_run() 호출 직전")
    stock_price_update_by_yfinance_run()

    print("디스코드 봇 실행 직전")
    run_discord_bot(TOKEN, CHANNEL_ID)