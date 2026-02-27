import feedparser
from supabase import create_client, Client
import os
import re
from datetime import datetime

# Connect to Supabase using GitHub Secrets
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# RSS feeds for Finnish business news
FEEDS = ["https://www.sttinfo.fi/uutishuone/rss"] 

ACQUISITION_KEYWORDS = ['ostaa', 'yrityskauppa', 'hankkii', 'merger', 'acquisition', 'yhdistyminen']
EXPANSION_KEYWORDS = ['laajentuu', 'tytäryhtiö', 'perustaa', 'expansion', 'subsidiary']

def categorize_activity(title):
    title_lower = title.lower()
    if any(word in title_lower for word in ACQUISITION_KEYWORDS):
        return "Acquisition"
    if any(word in title_lower for word in EXPANSION_KEYWORDS):
        return "Expansion"
    return None

print("Starting Finnish M&A scraper...")
articles_found = 0

for feed_url in FEEDS:
    print(f"Checking feed: {feed_url}")
    feed = feedparser.parse(feed_url)
    
    for entry in feed.entries:
        title = entry.title
        link = entry.link
        activity_type = categorize_activity(title)
        
        if activity_type:
            articles_found += 1
            company = title.split(' ')[0] # Basic extraction of company name
            try:
                dt = datetime(*entry.published_parsed[:6]).isoformat()
            except:
                dt = datetime.now().isoformat()
                
            # Insert into Supabase
            try:
                print(f"Attempting to add: {title}")
                supabase.table("finnish_ma_activities").insert({
                    "date": dt,
                    "company_name": company,
                    "title": title,
                    "url": link,
                    "activity_type": activity_type
                }).execute()
                print(f"✅ Successfully added to Supabase!")
            except Exception as e:
                # We removed the 'pass' and now print the actual error!
                print(f"❌ Failed to add to Supabase. Error: {e}")

if articles_found == 0:
    print("⚠️ No articles matched your M&A keywords today.")

print("Scraping complete.")
