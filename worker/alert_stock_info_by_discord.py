from dotenv import load_dotenv
import os
import discord
from discord.ext import tasks
from worker.get_low_di20_stocks import get_today_low_di20_stocks
from worker.format_utils import format_low_di20_stocks
# .Env파일 환경변수로 등록
load_dotenv()



def run_discord_bot(token: str, channel_id: int):
    intents = discord.Intents.default()
    intents.message_content = True
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        print(f'Logged in as {client.user}')
        check_low_di20_stock.start()

    @client.event
    async def on_message(message):
        if message.author == client.user:
            return
        if message.content == 'ping':
            await message.channel.send('pong!')

    @tasks.loop(minutes = 5)
    async def check_low_di20_stock():
        channel = client.get_channel(channel_id)
        low_di20_stocks = get_today_low_di20_stocks()
        if not low_di20_stocks:
            await channel.send("해당 조건에 맞는 종목이 없습니다.")
        else:
            msg = format_low_di20_stocks(low_di20_stocks)
            await channel.send(f"20일선 이격도 과대낙폭 종목 리스트:\n```\n{msg}\n```")

    client.run(token)