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

# 1. Google News RSS for the Finnish Paywalled Sites
RSS_FEEDS = [
    "https://news.google.com/rss/search?q=site:kauppalehti.fi+OR+site:talouselama.fi+OR+site:arvopaperi.fi&hl=fi&gl=FI&ceid=FI:fi"
]

# 2. Your Custom MFN Filtered Web Page
MFN_URL = "https://www.mfn.se/all/s/nordic?filter=(and(or(a.market_segment_ids%40%3E%5B17%5D)(a.market_segment_ids%40%3E%5B18%5D)(a.market_segment_ids%40%3E%5B19%5D)(a.market_segment_ids%40%3E%5B6%5D)))&limit=20000"

ACQUISITION_KEYWORDS = ['ostaa', 'yrityskauppa', 'hankkii', 'merger', 'acquisition', 'yhdistyminen', 'förvärv', 'förvärvar']
EXPANSION_KEYWORDS = ['laajentuu', 'tytäryhtiö', 'perustaa', 'expansion', 'subsidiary', 'etablerar']

def categorize_activity(title):
    title_lower = title.lower()
    if any(word in title_lower for word in ACQUISITION_KEYWORDS):
        return "Acquisition"
    if any(word in title_lower for word in EXPANSION_KEYWORDS):
        return "Expansion"
    return None

print("Starting Nordic M&A scraper...")
articles_found = 0

# --- PART 1: SCRAPE RSS FEEDS (Finnish Sites) ---
for feed_url in RSS_FEEDS:
    print(f"Checking Google News RSS...")
    feed = feedparser.parse(feed_url)
    print(f"🔍 I see a total of {len(feed.entries)} articles in the Google News feed.")
    
    for entry in feed.entries:
        title = entry.title
        link = entry.link
        activity_type = categorize_activity(title)
        
        if activity_type:
            articles_found += 1
            company = title.split(' ')[0] 
            try:
                dt = datetime(*entry.published_parsed[:6]).isoformat()
            except:
                dt = datetime.now().isoformat()
                
            try:
                supabase.table("finnish_ma_activities").insert({
                    "date": dt,
                    "company_name": company,
                    "title": title,
                    "url": link,
                    "activity_type": activity_type
                }).execute()
                print(f"✅ Added from RSS: '{title}'")
            except Exception as e:
                if "duplicate key value" not in str(e).lower():
                    pass # Ignore duplicate warnings silently

# --- PART 2: SCRAPE CUSTOM MFN HTML PAGE ---
print(f"Checking custom MFN page...")
try:
    # We use headers so the website thinks we are a normal web browser, not a bot!
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    response = requests.get(MFN_URL, headers=headers)
    
    # BeautifulSoup parses the HTML code
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all hyperlinks on the page
    links = soup.find_all('a', href=True)
    mfn_articles = []
    
    for a in links:
        link_url = a['href']
        title = a.get_text(strip=True)
        
        # Filter to make sure it's an actual news article link and not a menu button
        if title and len(title) > 20 and ("/a/" in link_url or "/one/" in link_url):
            if not link_url.startswith('http'):
                link_url = "https://www.mfn.se" + link_url
            mfn_articles.append({"title": title, "link": link_url})
            
    # Remove any accidental duplicates grabbed from the page
    unique_mfn_articles = {v['link']:v for v in mfn_articles}.values()
    print(f"🔍 I see {len(unique_mfn_articles)} possible articles on the MFN page.")
    
    for article in unique_mfn_articles:
        title = article['title']
        link = article['link']
        activity_type = categorize_activity(title)
        
        if activity_type:
            articles_found += 1
            company = title.split(' ')[0]
            dt = datetime.now().isoformat()
            
            try:
                supabase.table("finnish_ma_activities").insert({
                    "date": dt,
                    "company_name": company,
                    "title": title,
                    "url": link,
                    "activity_type": activity_type
                }).execute()
                print(f"✅ Added from MFN: '{title}'")
            except Exception as e:
                if "duplicate key value" not in str(e).lower():
                    pass
except Exception as e:
    print(f"❌ Error scraping MFN: {e}")

if articles_found == 0:
    print("⚠️ No M&A articles matched your keywords today.")

print(f"Scraping complete. Found {articles_found} M&A articles in total.")
