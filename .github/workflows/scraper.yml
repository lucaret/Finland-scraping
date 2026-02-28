import feedparser
from supabase import create_client, Client
import os
import re
from datetime import datetime

# Connect to Supabase using GitHub Secrets
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# Use Google News RSS to securely aggregate your target sites
FEEDS = [
    # Scrapes Kauppalehti, Talouselämä, and Arvopaperi in Finnish
    "https://news.google.com/rss/search?q=site:kauppalehti.fi+OR+site:talouselama.fi+OR+site:arvopaperi.fi&hl=fi&gl=FI&ceid=FI:fi",
    # Scrapes MFN Nordic
    "https://news.google.com/rss/search?q=site:mfn.se&hl=sv&gl=SE&ceid=SE:sv"
]

# The real M&A keywords (Added Swedish/English terms for MFN Nordic!)
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

for feed_url in FEEDS:
    print(f"Checking feed...")
    feed = feedparser.parse(feed_url)
    
    print(f"🔍 I see a total of {len(feed.entries)} articles in this feed right now.")
    
    for entry in feed.entries:
        title = entry.title
        link = entry.link
        activity_type = categorize_activity(title)
        
        if activity_type:
            articles_found += 1
            # Cleans up the Google News title formatting
            company = title.split(' ')[0] 
            try:
                dt = datetime(*entry.published_parsed[:6]).isoformat()
            except:
                dt = datetime.now().isoformat()
                
            # Insert into Supabase
            try:
                print(f"Attempting to add: '{title}'")
                supabase.table("finnish_ma_activities").insert({
                    "date": dt,
                    "company_name": company,
                    "title": title,
                    "url": link,
                    "activity_type": activity_type
                }).execute()
                print(f"✅ Successfully added to Supabase!")
            except Exception as e:
                # If the error is a duplicate URL, we stay quiet. Otherwise, print it!
                if "duplicate key value" not in str(e).lower():
                    print(f"❌ Failed to add to Supabase. Error: {e}")

if articles_found == 0:
    print("⚠️ No articles matched your M&A keywords today.")

print(f"Scraping complete. Found {articles_found} M&A articles in total.")
