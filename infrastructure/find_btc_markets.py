
import requests
import json
from datetime import datetime, timedelta, timezone

def get_window_ts(dt):
    # Round down to nearest 15
    minute = dt.minute
    window_start = minute - (minute % 15)
    start_dt = dt.replace(minute=window_start, second=0, microsecond=0)
    return int(start_dt.timestamp())

def find_markets():
    now = datetime.now(timezone.utc)
    current_ts = get_window_ts(now)
    next_ts = current_ts + (15 * 60)
    
    slugs = [
        f"btc-updown-15m-{current_ts}",
        f"btc-updown-15m-{next_ts}"
    ]
    
    print(f"Checking slugs: {slugs}")
    
    for slug in slugs:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        try:
            print(f"Fetching {url}...")
            response = requests.get(url)
            data = response.json()
            
            # Gamma usually returns list of one event if slug match
            if isinstance(data, list) and len(data) > 0:
                print(f"FOUND MATCH for {slug}!")
                event = data[0]
                print(f"Title: {event.get('title')}")
                markets = event.get('markets', [])
                for m in markets:
                    print(f"Market ID: {m.get('id')}")
                    print(f"Question: {m.get('question')}")
                    print(f"Tokens: {m.get('clobTokenIds')}")
            else:
                print(f"No match for {slug}")
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    find_markets()
