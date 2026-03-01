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

# --- MASTER CONFIGURATION: SOURCES BY COUNTRY ---
EUROPE_DATA = {
    "Finland": {
        "mfn_url": "https://www.mfn.se/all/s/nordic?filter=(and(or(a.market_segment_ids%40%3E%5B17%5D)(a.market_segment_ids%40%3E%5B18%5D)(a.market_segment_ids%40%3E%5B19%5D)(a.market_segment_ids%40%3E%5B6%5D)))&limit=20000",
        "cision_url": "https://news.cision.com/fi/",
        "rss_feeds": {
            "Alma Media (Kauppalehti/Talouselämä)": "https://news.google.com/rss/search?q=site:kauppalehti.fi+OR+site:talouselama.fi+OR+site:arvopaperi.fi&hl=fi&gl=FI&ceid=FI:fi",
            "STT Info": "https://news.google.com/rss/search?q=site:sttinfo.fi&hl=fi&gl=FI&ceid=FI:fi",
            "GlobeNewswire Finland": "https://news.google.com/rss/search?q=site:globenewswire.com+Finland&hl=fi-FI&gl=FI&ceid=FI:fi",
            "Google News ostaa": "https://news.google.com/rss/search?q=ostaa&hl=fi-FI&gl=FI&ceid=FI:fi",
            "Google News Yrityskauppa": "https://news.google.com/rss/search?q=Yrityskauppa&hl=fi-FI&gl=FI&ceid=FI:fi",
            "Google News Yritysosto": "https://news.google.com/rss/search?q=Yritysosto&hl=fi-FI&gl=FI&ceid=FI:fi",
            "Google News Haltuunotto": "https://news.google.com/rss/search?q=Haltuunotto&hl=fi-FI&gl=FI&ceid=FI:fi",
            "Inderes": "https://news.google.com/rss/search?q=site:inderes.fi&hl=fi&gl=FI&ceid=FI:fi",
            "Business Finland": "https://news.google.com/rss/search?q=site:businessfinland.fi&hl=fi&gl=FI&ceid=FI:fi",
            "Nordic Tech (ArcticStartup/Sifted)": "https://news.google.com/rss/search?q=site:arcticstartup.com+OR+site:sifted.eu+Finland&hl=en-US&gl=US&ceid=US:en"
        }
    },
    "Sweden": {
        "mfn_url": "https://mfn.se/all/s/nordic?filter=(and(or(a.market_segment_ids%40%3E%5B13%5D)(a.market_segment_ids%40%3E%5B14%5D)(a.market_segment_ids%40%3E%5B15%5D)(a.market_segment_ids%40%3E%5B5%5D)(a.market_segment_ids%40%3E%5B9%5D)(a.market_segment_ids%40%3E%5B1%5D)(a.market_segment_ids%40%3E%5B45%5D)(a.market_segment_ids%40%3E%5B44%5D)))&limit=1000000",
        "rss_feeds": {
            "Dagens Industri / Breakit": "https://news.google.com/rss/search?q=site:di.se+OR+site:breakit.se+OR+site:realtid.se&hl=sv&gl=SE&ceid=SE:sv",
            "GlobeNewswire Sweden": "https://news.google.com/rss/search?q=site:globenewswire.com+Sweden&hl=en-US&gl=US&ceid=US:en",
            "Cision Sweden": "https://news.google.com/rss/search?q=site:news.cision.com/se&hl=sv&gl=SE&ceid=SE:sv"
        }
    },
    "Denmark": {
        "mfn_url": "https://mfn.se/all/s/nordic?filter=(and(or(a.market_segment_ids%40%3E%5B24%5D)(a.market_segment_ids%40%3E%5B25%5D)(a.market_segment_ids%40%3E%5B26%5D)(a.market_segment_ids%40%3E%5B7%5D)(a.market_segment_ids%40%3E%5B10%5D)))&limit=1000000",
        "rss_feeds": {
            "Børsen / Finans": "https://news.google.com/rss/search?q=site:borsen.dk+OR+site:finans.dk+OR+site:berlingske.dk&hl=da&gl=DK&ceid=DK:da",
            "GlobeNewswire Denmark": "https://news.google.com/rss/search?q=site:globenewswire.com+Denmark&hl=en-US&gl=US&ceid=US:en",
            "Ritzau (Danish PR)": "https://news.google.com/rss/search?q=site:ritzau.dk&hl=da&gl=DK&ceid=DK:da"
        }
    },
    "Norway": {
        "mfn_url": "https://mfn.se/all/s/nordic?filter=(and(or(a.market_segment_ids%40%3E%5B56%5D)(a.market_segment_ids%40%3E%5B3%5D)(a.market_segment_ids%40%3E%5B43%5D)))&limit=1000000",
        "rss_feeds": {
            "DN / E24 / Finansavisen": "https://news.google.com/rss/search?q=site:dn.no+OR+site:e24.no+OR+site:finansavisen.no&hl=no&gl=NO&ceid=NO:no",
            "GlobeNewswire Norway": "https://news.google.com/rss/search?q=site:globenewswire.com+Norway&hl=en-US&gl=US&ceid=US:en",
            "NTB (Norwegian PR)": "https://news.google.com/rss/search?q=site:ntb.no&hl=no&gl=NO&ceid=NO:no"
        }
    },
    "Netherlands": {
        "rss_feeds": {
            "Het Financieele Dagblad / BNR": "https://news.google.com/rss/search?q=site:fd.nl+OR+site:bnr.nl+OR+site:iex.nl+OR+site:mtsprout.nl&hl=nl&gl=NL&ceid=NL:nl",
            "GlobeNewswire Netherlands": "https://news.google.com/rss/search?q=site:globenewswire.com+Netherlands&hl=en-US&gl=US&ceid=US:en",
            "ANP (Dutch PR)": "https://news.google.com/rss/search?q=site:anp.nl&hl=nl&gl=NL&ceid=NL:nl"
        }
    },
    "Belgium": {
        "rss_feeds": {
            "De Tijd / Trends (NL)": "https://news.google.com/rss/search?q=site:tijd.be+OR+site:trends.knack.be&hl=nl&gl=BE&ceid=BE:nl",
            "L'Echo / Le Vif (FR)": "https://news.google.com/rss/search?q=site:lecho.be+OR+site:trends.levif.be&hl=fr&gl=BE&ceid=BE:fr",
            "GlobeNewswire Belgium": "https://news.google.com/rss/search?q=site:globenewswire.com+Belgium&hl=en-US&gl=US&ceid=US:en"
        }
    },
    "France": {
        "rss_feeds": {
            "Les Echos / La Tribune": "https://news.google.com/rss/search?q=site:lesechos.fr+OR+site:latribune.fr+OR+site:lefigaro.fr/economie&hl=fr&gl=FR&ceid=FR:fr",
            "GlobeNewswire France": "https://news.google.com/rss/search?q=site:globenewswire.com+France&hl=en-US&gl=US&ceid=US:en",
            "L'Usine Nouvelle / French Tech": "https://news.google.com/rss/search?q=site:usinenouvelle.com+OR+site:frenchweb.fr&hl=fr&gl=FR&ceid=FR:fr"
        }
    }
}

# --- PAN-EUROPEAN KEYWORDS ---
# Added Dutch (NL) and French (FR) terminology
ACQUISITION_KEYWORDS = [
    'ostaa', 'yrityskauppa', 'hankkii', 'yhdistyminen', 'Haltuunotto', 'Yritysosto' # FI
    'förvärv', 'förvärvar', 'köper', 'samgåendes',      # SV
    'opkøb', 'køber', 'fusion',                         # DA
    'oppkjøp', 'kjøper', 'fusjon',                      # NO
    'overname', 'neemt over', 'koopt', 'acquisitie',    # NL
    'rachat', 'acquiert', 'acquisition', 'rachète',     # FR
    'merger', 'acquisition'                             # EN
]
EXPANSION_KEYWORDS = [
    'laajentuu', 'tytäryhtiö', 'perustaa',              # FI
    'etablerar', 'dotterbolag',                         # SV
    'ekspansion', 'etablerer', 'datterselskab',         # DA
    'ekspansjon', 'datterselskap',                      # NO
    'uitbreiding', 'opent', 'dochteronderneming',       # NL
    'expansion', 'filiale', "s'étend", 'ouverture',     # FR
    'subsidiary'                                        # EN
]
EXCLUSION_KEYWORDS = [
    'own shares', 'omien osakkeiden', 'omia osakkeita', # EN/FI
    'egna aktier', 'återköp',                           # SV
    'egne aktier', 'tilbagekøb',                        # DA
    'egne aksjer', 'tilbakekjøp',                       # NO
    'eigen aandelen', 'inkoop eigen aandelen',          # NL
    'actions propres', "rachat d'actions"               # FR
]

def categorize_activity(title):
    title_lower = title.lower()
    if any(word in title_lower for word in EXCLUSION_KEYWORDS):
        return None
    if any(word in title_lower for word in ACQUISITION_KEYWORDS):
        return "Acquisition"
    if any(word in title_lower for word in EXPANSION_KEYWORDS):
        return "Expansion"
    return None

print("Starting Pan-European M&A scraper...")
articles_found = 0
source_counts = {} 
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# --- LOOP THROUGH EACH COUNTRY ---
for country, config in EUROPE_DATA.items():
    print(f"\n=======================")
    print(f"🌍 SEARCHING: {country}")
    print(f"=======================")

    # 1. Scrape RSS Feeds for this country
    for source_name, feed_url in config.get("rss_feeds", {}).items():
        print(f"Checking {source_name}...")
        tracker_name = f"{source_name} ({country})"
        source_counts[tracker_name] = 0
        feed = feedparser.parse(feed_url)
        
        for entry in feed.entries:
            title = entry.title
            link = entry.link
            activity_type = categorize_activity(title)
            
            if activity_type:
                articles_found += 1
                source_counts[tracker_name] += 1
                try:
                    dt = datetime(*entry.published_parsed[:6]).isoformat()
                except:
                    dt = datetime.now().isoformat()
                    
                try:
                    supabase.table("finnish_ma_activities").insert({
                        "country": country,
                        "date": dt,
                        "title": title,
                        "url": link,
                        "activity_type": activity_type,
                        "source": source_name
                    }).execute()
                    print(f"✅ Added: '{title}'")
                except Exception as e:
                    if "duplicate key value" not in str(e).lower():
                        print(f"❌ Supabase Error: {e}")

    # 2. Scrape MFN Nordic HTML for this country
    if config.get("mfn_url"):
        source_name = "MFN Nordic"
        print(f"Checking {source_name} ({country})...")
        tracker_name = f"{source_name} ({country})"
        source_counts[tracker_name] = 0
        
        try:
            response = requests.get(config["mfn_url"], headers=headers)
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
                    source_counts[tracker_name] += 1
                    try:
                        supabase.table("finnish_ma_activities").insert({
                            "country": country,
                            "date": datetime.now().isoformat(),
                            "title": title,
                            "url": link,
                            "activity_type": activity_type,
                            "source": source_name
                        }).execute()
                        print(f"✅ Added: '{title}'")
                    except Exception as e:
                        if "duplicate key value" not in str(e).lower():
                            print(f"❌ Supabase Error: {e}")
        except Exception as e:
            print(f"❌ Error scraping MFN {country}: {e}")

    # 3. Scrape specific HTML sources (Like Cision FI)
    if config.get("cision_url"):
        source_name = "Cision HTML"
        print(f"Checking {source_name} ({country})...")
        tracker_name = f"{source_name} ({country})"
        source_counts[tracker_name] = 0
        
        try:
            response = requests.get(config["cision_url"], headers=headers)
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
                    source_counts[tracker_name] += 1
                    try:
                        supabase.table("finnish_ma_activities").insert({
                            "country": country,
                            "date": datetime.now().isoformat(),
                            "title": title,
                            "url": link,
                            "activity_type": activity_type,
                            "source": source_name
                        }).execute()
                        print(f"✅ Added: '{title}'")
                    except Exception as e:
                        if "duplicate key value" not in str(e).lower():
                            print(f"❌ Supabase Error: {e}")
        except Exception as e:
            print(f"❌ Error scraping Cision {country}: {e}")

# --- FINAL SCORECARD ---
print("\n--- PAN-EUROPEAN SCRAPE SUMMARY ---")
for source, count in source_counts.items():
    if count > 0:
        print(f"{source}: {count} M&A articles found")
print(f"\nTotal: {articles_found} M&A articles found across all countries.")
