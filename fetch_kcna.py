import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import random
import os
import time
from datetime import datetime
from tqdm import tqdm

# Load the current headlines archive from S3
archive_url = 'https://stilesdata.com/north-korea-news/headlines.json'
current_archive = pd.read_json(archive_url, orient='records')

# Convert date column in current archive to datetime
current_archive['date'] = pd.to_datetime(current_archive['date'], errors='raise')

today = pd.Timestamp.today().strftime('%Y-%m-%d')
key_topics = [
    "WPK General Secretary Kim Jong Un's Revolutionary Activities", "Documents",
    'Latest News', 'Top News', 'Home News', 'World', "Revolutionary Anecdote",
    'Society-Life', 'External', 'News Commentary', 'Always in Memory of People', 'Celebrations for New Year'
]

# Retrieve the proxy service key from environment variables (GitHub Actions secrets)
proxy_service_key = os.getenv('SCRAPE_PROXY_KEY')

# List of user-agents for rotation
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/78.0',
]

# Create a session with retry strategy
def create_session_with_retries():
    """Create a requests session with automatic retry logic for transient failures"""
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=5,  # Total number of retries
        backoff_factor=2,  # Wait 1s, 2s, 4s, 8s, 16s between retries
        status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry
        allowed_methods=["HEAD", "GET", "OPTIONS"],  # Methods to retry
        raise_on_status=False  # Don't raise exception on max retries
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

# Create global session for all requests
session = create_session_with_retries()

# Function to fetch the menu links
def fetch_menu_links(url, max_attempts=3):
    headers = {
        'User-Agent': random.choice(user_agents)
    }
    
    for attempt in range(max_attempts):
        try:
            response = session.get(
                'https://proxy.scrapeops.io/v1/',
                params={
                    'api_key': proxy_service_key,
                    'url': url,
                    'premium': 'true'
                },
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                menu_block = soup.find('div', class_='col-md-12 menu-block')
                links_data = []

                if menu_block:
                    links = menu_block.find_all('a')
                    for link in links:
                        topic = link.text.strip()
                        link_url = link.get('href')
                        full_url = f"http://www.kcna.kp{link_url}"
                        links_data.append({'topic': topic, 'link': full_url})

                return pd.DataFrame(links_data)
            else:
                print(f"Failed to retrieve menu page. Status code: {response.status_code}")
                if attempt < max_attempts - 1:
                    time.sleep(5 * (attempt + 1))
                    
        except Exception as e:
            print(f"Error fetching menu links (attempt {attempt + 1}/{max_attempts}): {e}")
            if attempt < max_attempts - 1:
                time.sleep(5 * (attempt + 1))
            else:
                print("Max attempts reached for menu links")
    
    return pd.DataFrame()

# Revised function to distinguish between Juche and Gregorian dates
# def convert_juche_to_gregorian(juche_date):
#     if juche_date is None or juche_date == 'Unknown':
#         return pd.NaT
    
#     try:
#         # Check if date starts with "Juche" to identify Juche format
#         if "Juche" in juche_date:
#             clean_date = juche_date.replace('[', '').replace(']', '')
#             parts = clean_date.split('.')
            
#             # Extract Juche year
#             juche_year = int(parts[0].replace('Juche', '').strip())
#             month = int(parts[1])
#             day = int(parts[2])

#             # Convert Juche year to Gregorian year
#             gregorian_year = juche_year + 1911
#         else:
#             # Treat as Gregorian date
#             parts = juche_date.split('.')
#             gregorian_year = int(parts[0])
#             month = int(parts[1])
#             day = int(parts[2])

#         # Print for debugging purposes
#         print(f"Converting date: {juche_date} => Year: {gregorian_year}, Month: {month}, Day: {day}")

#         # Validate date range
#         if not (1900 <= gregorian_year <= datetime.now().year):
#             print(f"Out of bounds date: {gregorian_year}-{month}-{day}")
#             return pd.NaT

#         return datetime(gregorian_year, month, day)

#     except Exception as e:
#         print(f"Error converting date: {juche_date} - {e}")
#         return pd.NaT

def convert_to_gregorian(date_str):
    if date_str is None or date_str == 'Unknown':
        return pd.NaT
    
    try:
        # Clean the date string by removing brackets
        clean_date = date_str.replace('[', '').replace(']', '')
        
        # Split the date into year, month, and day
        parts = clean_date.split('.')
        year = int(parts[0])
        month = int(parts[1])
        day = int(parts[2])

        # Debug print for verification
        print(f"Converting date: {date_str} => Year: {year}, Month: {month}, Day: {day}")

        # Ensure the date falls within a reasonable range
        if not (1900 <= year <= datetime.now().year):
            print(f"Out of bounds date: {year}-{month}-{day}")
            return pd.NaT

        return datetime(year, month, day)

    except Exception as e:
        print(f"Error converting date: {date_str} - {e}")
        return pd.NaT



# Function to parse articles and media from topic pages
def parse_articles(page_url, topic, max_attempts=3):
    headers = {
        'User-Agent': random.choice(user_agents)
    }
    
    for attempt in range(max_attempts):
        try:
            response = session.get(
                'https://proxy.scrapeops.io/v1/',
                params={
                    'api_key': proxy_service_key,
                    'url': page_url,
                    'premium': 'true'
                },
                headers=headers,
                timeout=30
            )
            
            articles = []
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                # Extract articles
                article_lists = soup.find_all('ul', class_='article-link')
                for article_list in article_lists:
                    article_links = article_list.find_all('li')[:5]  # Limit to first 5 articles
                    for article in article_links:
                        a_tag = article.find('a')
                        if a_tag:
                            headline = a_tag.text.strip().split('\r')[0]
                            link = a_tag.get('href')
                            date_tag = article.find('span', class_='publish-time')
                            date = date_tag.text.strip() if date_tag else 'Unknown'
                            full_link = f"http://www.kcna.kp{link}"
                            articles.append({
                                'topic': topic,
                                'headline': headline,
                                'link': full_link,
                                'date': convert_to_gregorian(date)
                            })

                # Extract photos and videos
                if topic in ['Photo', 'Video']:
                    media_divs = soup.find_all('div', class_=['photo', 'video'])
                    for media in media_divs[:5]:  # Limit to first 5 items
                        title_span = media.find('span', class_='title')
                        if title_span:
                            a_tag = title_span.find('a')
                            headline = a_tag.text.strip().split('\r')[0] if a_tag else 'Unknown'
                            link = a_tag.get('href') if a_tag else 'Unknown'
                            date_tag = title_span.find('span', class_='publish-time')
                            date = date_tag.text.strip() if date_tag else 'Unknown'
                            full_link = f"http://www.kcna.kp{link}"
                            articles.append({
                                'topic': topic,
                                'headline': headline,
                                'link': full_link,
                                'date': convert_to_gregorian(date)
                            })

                return articles
            else:
                print(f"Failed to retrieve topic page: {page_url}. Status code: {response.status_code}")
                if attempt < max_attempts - 1:
                    time.sleep(3 * (attempt + 1))
                    
        except Exception as e:
            print(f"Error parsing articles from {page_url} (attempt {attempt + 1}/{max_attempts}): {e}")
            if attempt < max_attempts - 1:
                time.sleep(3 * (attempt + 1))
            else:
                print(f"Max attempts reached for {page_url}, skipping")

    return []

# Function to extract story text from article links
def fetch_story_text(link, max_attempts=3):
    headers = {
        'User-Agent': random.choice(user_agents)
    }
    
    for attempt in range(max_attempts):
        try:
            # Add small delay between requests to avoid overwhelming the proxy
            time.sleep(0.5)
            
            response = session.get(
                'https://proxy.scrapeops.io/v1/',
                params={
                    'api_key': proxy_service_key,
                    'url': link,
                    'premium': 'true'
                },
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                content_wrapper = soup.find('div', class_='content-wrapper')
                if content_wrapper:
                    paragraphs = content_wrapper.find_all('p')
                    story_text = "\n".join(p.get_text(strip=True) for p in paragraphs)
                    return story_text
                return ''
            else:
                print(f"Failed to fetch story text. Status code: {response.status_code}")
                if attempt < max_attempts - 1:
                    time.sleep(3 * (attempt + 1))
                    
        except Exception as e:
            print(f"Error fetching story text (attempt {attempt + 1}/{max_attempts}): {e}")
            if attempt < max_attempts - 1:
                time.sleep(3 * (attempt + 1))
            else:
                print(f"Max attempts reached for {link}, returning empty string")
    
    return ''

# Function to collect headlines and fetch story text
def collect_headlines_and_stories(links_df):
    all_articles = []

    for _, row in tqdm(links_df.iterrows(), total=links_df.shape[0], desc='Processing Pages'):
        topic = row['topic']
        link = row['link']

        # Parse the articles on the page
        articles = parse_articles(link, topic)
        
        # Fetch story text for key topics
        for article in articles:
            if article['topic'] in key_topics:
                story_text = fetch_story_text(article['link'])
                article['story_text'] = story_text
            else:
                article['story_text'] = ''

        all_articles.extend(articles)

    return pd.DataFrame(all_articles)

# Main workflow
menu_url = 'http://www.kcna.kp/en'  # Replace with the actual URL of the menu page
links_df = fetch_menu_links(menu_url)
headlines_df = collect_headlines_and_stories(links_df)

print("Unique dates before conversion:", headlines_df['date'].unique())

# Convert date column in headlines_df to datetime
headlines_df['date'] = pd.to_datetime(headlines_df['date'], errors='raise')

# Combine with current archive, remove duplicates, and sort by date
all_headlines_df = pd.concat([current_archive, headlines_df])\
                      .drop_duplicates(subset=['headline', 'link'])\
                      .sort_values(by='date')\
                      .reset_index(drop=True)

# Export links and headlines. Save a dated copy in an archive directory.
all_headlines_df['date_str'] = pd.to_datetime(all_headlines_df['date']).dt.strftime('%Y-%m-%d')

all_headlines_df.to_json('data/headlines.json', indent=4, orient='records')
all_headlines_df.to_json(f'data/archive/headlines_{today}.json', indent=4, orient='records')

links_df.to_json('data/links.json', indent=4, orient='records')
links_df.to_json(f'data/archive/links_{today}.json', indent=4, orient='records')