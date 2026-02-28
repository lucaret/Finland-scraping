import feedparser
from supabase import create_client, Client
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# Connect to Supabase using GitHub Secrets
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# 1. Google News RSS Feeds
RSS_SOURCES = {
    "Alma Media (Kauppalehti/Talouselämä)": "https://news.google.com/rss/search?q=site:kauppalehti.fi+OR+site:talouselama.fi+OR+site:arvopaperi.fi&hl=fi&gl=FI&ceid=FI:fi",
    "STT Info": "https://news.google.com/rss/search?q=site:sttinfo.fi&hl=fi&gl=FI&ceid=FI:fi",
    "GlobeNewswire": "https://news.google.com/rss/search?q=site:globenewswire.com+Finland&hl=en-US&gl=US&ceid=US:en",
    "Inderes": "https://news.google.com/rss/search?q=site:inderes.fi&hl=fi&gl=FI&ceid=FI:fi",
    "Business Finland": "https://news.google.com/rss/search?q=site:businessfinland.fi&hl=fi&gl=FI&ceid=FI:fi",
    "Nordic Tech (ArcticStartup/Sifted)": "https://news.google.com/rss/search?q=site:arcticstartup.com+OR+site:sifted.eu+Finland&hl=en-US&gl=US&ceid=US:en"
}

# 2. Custom HTML Web Pages
MFN_URL = "https://www.mfn.se/all/s/nordic?filter=(and(or(a.market_segment_ids%40%3E%5B17%5D)(a.market_segment_ids%40%3E%5B18%5D)(a.market_segment_ids%40%3E%5B19%5D)(a.market_segment_ids%40%3E%5B6%5D)))&limit=20000"
CISION_URL = "https://news.cision.com/fi/"

# Keywords
ACQUISITION_KEYWORDS = ['ostaa', 'yrityskauppa', 'hankkii', 'merger', 'acquisition', 'yhdistyminen', 'förvärv', 'förvärvar']
EXPANSION_KEYWORDS = ['laajentuu', 'tytäryhtiö', 'perustaa', 'expansion', 'subsidiary', 'etablerar']
EXCLUSION_KEYWORDS = ['own shares', 'omien osakkeiden', 'omia osakkeita', 'egna aktier']

def categorize_activity(title):
    title_lower = title.lower()
    if any(word in title_lower for word in EXCLUSION_KEYWORDS):
        return None
    if any(word in title_lower for word in ACQUISITION_KEYWORDS):
        return "Acquisition"
    if any(word in title_lower for word in EXPANSION_KEYWORDS):
        return "Expansion"
    return None

print("Starting Ultimate Nordic M&A scraper...")
articles_found = 0
source_counts = {} 
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# --- PART 1: SCRAPE RSS FEEDS ---
for source_name, feed_url in RSS_SOURCES.items():
    print(f"Checking {source_name}...")
    source_counts[source_name] = 0
    feed = feedparser.parse(feed_url)
    
    for entry in feed.entries:
        title = entry.title
        link = entry.link
        activity_type = categorize_activity(title)
        
        if activity_type:
            articles_found += 1
            source_counts[source_name] += 1
            try:
                dt = datetime(*entry.published_parsed[:6]).isoformat()
            except:
                dt = datetime.now().isoformat()
                
            try:
                supabase.table("finnish_ma_activities").insert({
                    "date": dt,
                    "title": title,
                    "url": link,
                    "activity_type": activity_type,
                    "source": source_name
                }).execute()
                print(f"✅ Added from {source_name}: '{title}'")
            except Exception as e:
                if "duplicate key value" not in str(e).lower():
                    print(f"❌ Supabase Error: {e}")

# --- PART 2: SCRAPE MFN NORDIC HTML ---
print(f"Checking MFN Nordic...")
source_name = "MFN Nordic"
source_counts[source_name] = 0
try:
    response = requests.get(MFN_URL, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=True)
    mfn_articles = []
    
    for a in links:
        link_url = a['href']
        title = a.get_text(strip=True)
        if title and len(title) > 20 and ("/a/" in link_url or "/one/" in link_url):
            if not link_url.startswith('http'):
                link_url = "https://www.mfn.se" + link_url
            mfn_articles.append({"title": title, "link": link_url})
            
    for article in {v['link']:v for v in mfn_articles}.values():
        title = article['title']
        link = article['link']
        activity_type = categorize_activity(title)
        
        if activity_type:
            articles_found += 1
            source_counts[source_name] += 1
            try:
                supabase.table("finnish_ma_activities").insert({
                    "date": datetime.now().isoformat(),
                    "title": title,
                    "url": link,
                    "activity_type": activity_type,
                    "source": source_name
                }).execute()
                print(f"✅ Added from {source_name}: '{title}'")
            except Exception as e:
                if "duplicate key value" not in str(e).lower():
                    print(f"❌ Supabase Error: {e}")
except Exception as e:
    print(f"❌ Error scraping MFN: {e}")

# --- PART 3: SCRAPE CISION FINLAND HTML ---
print(f"Checking Cision Finland...")
source_name = "Cision Finland"
source_counts[source_name] = 0
try:
    response = requests.get(CISION_URL, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=True)
    cision_articles = []
    
    for a in links:
        link_url = a['href']
        title = a.get_text(strip=True)
        if title and len(title) > 15 and "/r/" in link_url:
            if not link_url.startswith('http'):
                link_url = "https://news.cision.com" + link_url
            cision_articles.append({"title": title, "link": link_url})
            
    for article in {v['link']:v for v in cision_articles}.values():
        title = article['title']
        link = article['link']
        activity_type = categorize_activity(title)
        
        if activity_type:
            articles_found += 1
            source_counts[source_name] += 1
            try:
                supabase.table("finnish_ma_activities").insert({
                    "date": datetime.now().isoformat(),
                    "title": title,
                    "url": link,
                    "activity_type": activity_type,
                    "source": source_name
                }).execute()
                print(f"✅ Added from {source_name}: '{title}'")
            except Exception as e:
                if "duplicate key value" not in str(e).lower():
                    print(f"❌ Supabase Error: {e}")
except Exception as e:
    print(f"❌ Error scraping Cision: {e}")

print("\n--- SCRAPE SUMMARY ---")
for source, count in source_counts.items():
    print(f"{source}: {count} M&A articles found")
print(f"Total: {articles_found} M&A articles found across all sources.")
