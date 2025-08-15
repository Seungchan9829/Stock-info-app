import os, json, time, asyncio
import aiohttp, websockets
from dotenv import load_dotenv

load_dotenv()  # ← dotenv 적용

SPOT_REST = "https://data-api.binance.vision"
SPOT_WS   = "wss://data-stream.binance.vision/stream"

DISCORD_WEBHOOK_COIN = os.getenv("DISCORD_WEBHOOK_COIN")  # ← env 키 확인해서 통일하세요
EXINFO_TTL_SEC = 3600 # exchangeInfo 캐시 TTL(1h)
TOP_N = 50
CHUNK_SIZE = 100
REFRESH_MIN = 10
THRESHOLD = float(os.getenv("THRESHOLD"))

async def send_discord(session: aiohttp.ClientSession, content:str):
    if not DISCORD_WEBHOOK_COIN:
        print("디스코드 웹 훅이 세팅되지 않았습니다.\n", content)
        return
    try:
        await session.post(DISCORD_WEBHOOK_COIN, json={"content": content}, timeout=15)
    except Exception as e:
        print("디스코드 전송에 실패하였습니다:", e)


class SymbolCache:
    def __init__(self):
        self.usdt_symbols = []
        self.last_fetch = 0

    async def refresh_if_needed(self, session: aiohttp.ClientSession):  # ← __init__ 밖으로!
        now = time.time()
        if now - self.last_fetch < EXINFO_TTL_SEC and self.usdt_symbols:
            return self.usdt_symbols
        
        url = f"{SPOT_REST}/api/v3/exchangeInfo"
        async with session.get(url, timeout=30) as r:
            info = await r.json()

        syms = []
        for s in info.get("symbols", []):
            if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT":
                syms.append(s["symbol"])
        self.usdt_symbols = sorted(set(syms))
        self.last_fetch = now
        return self.usdt_symbols

class PrevDayCache:
    def __init__(self, ttl_sec = 6 * 3600):
        self.map = {}
        self.ttl = ttl_sec
        self.sem = asyncio.Semaphore(15)

    async def get(self, session : aiohttp.ClientSession, symbol: str) -> float:
        now = time.time()
        if symbol in self.map:
            q, ts = self.map[symbol]
            if now - ts < self.ttl:
                return q
        q = await self._fetch_prev_q(session, symbol)
        self.map[symbol] = (q, now)
        return q

    async def warm(self, session : aiohttp.ClientSession, symbols : list[str]):
        async def worker(sym):
            try:
                await self.get(session, sym)
            except Exception as e:
                print(f"[PREVQ] warm fail {sym}: {e}")

        await asyncio.gather(*[worker(s) for s in symbols])

    async def _fetch_prev_q(self, session: aiohttp.ClientSession, symbol : str) -> float:
        # /api/v3/klines?interval=1d&limit=2 → 마지막 바로 이전 봉의 q 사용
        url = f"{SPOT_REST}/api/v3/klines"
        params = {"symbol": symbol, "interval": "1d", "limit": 2}
        async with self.sem, session.get(url, params=params, timeout=20) as r:
            arr = await r.json()
        if not isinstance(arr, list) or len(arr) < 2:
            return 0.0
        prev = arr[-2]
        return float(prev[7] or 0.0)

def clean_symbols(symbols: list[str]) -> list[str]:
    cleaned = []
    for s in symbols:
        if not isinstance(s, str):
            continue
        s = s.strip().upper()
        # A-Z, 0-9, -, _, . 만 허용
        if s and all(c.isalnum() or c in "-_." for c in s):
            cleaned.append(s)
    return cleaned

async def fetch_24hr_chunk(session, symbols):
    url = f"{SPOT_REST}/api/v3/ticker/24hr"
    # 공백 제거 JSON 문자열로 직렬화
    params = {"symbols": json.dumps(symbols, separators=(',', ':'))}
    async with session.get(url, params=params, timeout=30) as r:
        data = await r.json()
        if isinstance(data, dict) and "code" in data:
            raise RuntimeError(f"24hr API error: {data}")
        return data
    
# 상위 N개의 거래대금을 가진 심볼을 반환   
async def get_top_by_quote_volume(session, usdt_symbols, top_n = TOP_N, chunk_size = CHUNK_SIZE):
    rows = []
    chunk_size = min(chunk_size, 100)  # 이중 안전장치
    for i in range(0, len(usdt_symbols), chunk_size):
        chunk = usdt_symbols[i:i+chunk_size]
        try:
            data = await fetch_24hr_chunk(session, chunk)

            if not isinstance(data, list):
                print("24시간 응답 형식이 리스트가 아닙니다:", data)
                continue

            for d in data:
                # d가 dict인지 확인
                if not isinstance(d, dict):
                    print("24시간 항목 형식 이상:", d)
                    continue
                qv = float(d.get("quoteVolume", "0") or 0.0)
                sym = d.get("symbol")
                if sym:
                    rows.append((sym, qv))
        except Exception as e:
            print("24시간 청크 실패:", e)  # 여기서 에러 dict도 찍힘
        await asyncio.sleep(0.2)
    
    rows.sort(key=lambda x: x[1], reverse=True)
    return [sym for sym, _ in rows[:top_n]]

class WatchlistManager:
    def __init__(self):
        self.current = set()
    
    def diff(self, new_list):
        new_set = set(new_list)
        to_add = list(new_set - self.current)
        to_del = list(self.current - new_set)
        self.current = new_set
        return to_add, to_del


async def subscribe(ws, symbols, interval="1h", id_base = 1000):
    if not symbols:
        return 
    params = [f"{s.lower()}@kline_{interval}" for s in symbols]
    msg = {"method":"SUBSCRIBE","params":params,"id":id_base}
    await ws.send(json.dumps(msg))
    await asyncio.sleep(max(0.2, len(params)/10.0))

async def unsubscribe(ws, symbols, interval="1h", id_base = 2000):
    if not symbols:
        return
    params = [f"{s.lower()}@kline_{interval}" for s in symbols]
    msg = {"method":"UNSUBSCRIBE","params":params,"id":id_base}
    await ws.send(json.dumps(msg))
    await asyncio.sleep(max(0.2, len(params)/10.0))


class HourlyAlertState:
    def __init__(self):
        self.state = {}

    def update_and_should_alert(self, symbol:str, k_start:int, crossed: bool) -> bool:  # ← int 로
        st = self.state.get(symbol)
        if not st or st["t"] != k_start:
            self.state[symbol] = {"t": k_start, "alerted": False}
            st = self.state[symbol]
        if crossed and not st["alerted"]:
            st["alerted"] = True
            return True
        return False


async def ws_loop(session, watchlist_mgr: WatchlistManager, ws_ctl_queue: asyncio.Queue, prevq_cache: PrevDayCache):
    url = f"{SPOT_WS}?streams="  # ← 등호 추가
    hstate = HourlyAlertState()

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=60, max_size=2**23) as ws:
                print("websocket 연결")
                if watchlist_mgr.current:
                    await subscribe(ws, list(watchlist_mgr.current))
                

                async def handle_ctl():
                    while True:
                        typ, to_add, to_del = await ws_ctl_queue.get()
                        if typ == "update":
                            if to_del:
                                await unsubscribe(ws, to_del)
                            if to_add:
                                await subscribe(ws, to_add)

                ctl_task = asyncio.create_task(handle_ctl())

                try:
                    async for raw in ws:
                        msg = json.loads(raw)
                        data = msg.get("data", msg)

                        if data.get("e") == "kline":
                            k = data.get("k", {})
                            symbol = (k.get("s") or "").upper()
                            if symbol not in watchlist_mgr.current:
                                continue

                            k_start = int(k.get("t", 0))
                            q_now   = float(k.get("q", "0") or 0.0)
                            is_closed = bool(k.get("x", False))

                            prev_q = await prevq_cache.get(session, symbol)
                            if prev_q <= 0:
                                continue

                            crossed = (q_now >= THRESHOLD * prev_q)
                            if hstate.update_and_should_alert(symbol, k_start, crossed):
                                ratio = q_now / prev_q if prev_q else 0.0
                                text = (
                                    f"🚨 **{symbol}** 1h 거래대금 급증 감지\n"
                                    f"- 현재 1h q: `{q_now:,.2f}`\n"
                                    f"- 전일 1d q: `{prev_q:,.2f}`\n"
                                    f"- 배수: `{ratio:.2f}x` (기준 {THRESHOLD}x)\n"
                                    f"- 캔들확정(x): {is_closed}\n"
                                )
                                await send_discord(session, text)
                finally:
                    ctl_task.cancel()

        except Exception as e:
            print("웹소켓 에러:", e)
            await asyncio.sleep(3)


async def watchlist_refresher(session, sym_cache: SymbolCache, watchlist_mgr : WatchlistManager, ws_ctl_queue: asyncio.Queue, prevq_cache: PrevDayCache):
    while True:
        try:
            usdt_syms = await sym_cache.refresh_if_needed(session)
            topN = await get_top_by_quote_volume(session, usdt_syms, TOP_N, CHUNK_SIZE)
            to_add, to_del = watchlist_mgr.diff(topN)
            if to_add or to_del:
                await prevq_cache.warm(session, to_add)
                await ws_ctl_queue.put(("update", to_add, to_del))
                print(f"[WATCHLIST] updated. +{len(to_add)} -{len(to_del)} total={len(watchlist_mgr.current)}")
        except Exception as e:
            print("WatchList 리프레시 실패:", e)
        await asyncio.sleep(REFRESH_MIN * 60)




async def main_coin_alert():
    sym_cache = SymbolCache()
    watchlist_mgr = WatchlistManager()
    ws_ctl_queue = asyncio.Queue()
    prevq_cache = PrevDayCache(ttl_sec = 6 * 3600)

    async with aiohttp.ClientSession() as session:  # ← () 추가
        await asyncio.gather(
            ws_loop(session, watchlist_mgr, ws_ctl_queue, prevq_cache),
            watchlist_refresher(session, sym_cache, watchlist_mgr, ws_ctl_queue, prevq_cache)
        )

if __name__ == "__main__":
    try:
        asyncio.run(main_coin_alert())
    except KeyboardInterrupt:
        print("종료합니다.")
