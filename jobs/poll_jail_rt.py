import asyncio, aiohttp, aioredis, os, datetime
from api.db import get_pool

JAIL_URL = "https://harriscountyso.org/api/bookings?minutes=15"
REDIS_URL = os.getenv("REDIS_URL")

async def poll_jail():
    """Poll Harris County jail roster every 15 minutes and cache booking numbers"""
    r = await aioredis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(JAIL_URL) as resp:
                    data = await resp.json()
                    
                for booking in data:
                    key = f"jail:{booking['booking_number']}"
                    await r.setex(key, 900, datetime.datetime.utcnow().isoformat())  # 15 min TTL
                    
                print(f"Polled {len(data)} bookings")
                await asyncio.sleep(900)  # 15 min
            except Exception as e:
                print(f"Error polling jail: {e}")
                await asyncio.sleep(60)  # retry in 1 min on error

if __name__ == "__main__":
    asyncio.run(poll_jail())
