import aiohttp, os, tldextract
from typing import Optional, List

WHOIS_URL = "https://www.whoisxmlapi.com/whoisserver/WhoisService"
WHOIS_KEY = os.getenv("WHOIS_KEY")  # free tier 500/mo

async def enrich_domain(email: str) -> Optional[List[str]]:
    """
    Returns domains registered to email (extracted from WHOIS).
    """
    if not email or "@" not in email:
        return None
    
    domain = email.split("@")[1]
    params = {
        "apiKey": WHOIS_KEY,
        "domainName": domain,
        "outputFormat": "JSON"
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(WHOIS_URL, params=params, timeout=15) as resp:
            if resp.status != 200:
                return None
            
            data = await resp.json()
            whois = data.get("WhoisRecord", {})
            # extract all domains in same registrant block
            raw_text = whois.get("rawText", "")
            domains = set(tldextract.extract(d).registered_domain for d in raw_text.split() if "." in d)
            return list(domains)[:10]  # cap at 10
