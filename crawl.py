import requests
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import traceback
import time
import random
import tqdm
from multiprocessing import Pool
import multiprocessing
import pandas as pd
import csv

def fetch_sitemap(sitemap_url):
    """
    Fetches the sitemap.xml and parses the links.
    """
    try:
        response = requests.get(sitemap_url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
        xml_content = response.text
        root = ET.fromstring(xml_content)
        namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}  # XML namespace
        urls = [url_elem.text for url_elem in root.findall('.//ns:loc', namespaces)]
        return urls
    except Exception as e:
        print(f"Error fetching or parsing sitemap: {e}")
        traceback.print_exc()
        return []

def extract_index_from_url(url):
  response = requests.get(url)
  response.raise_for_status()

  # Parse the XML content
  root = ET.fromstring(response.text)

  # Define the namespace
  namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

  # Extract all <loc> elements
  links = [loc.text for loc in root.findall('.//ns:loc', namespace)]

  # Print the extracted links
  for link in links:
      print(link)
      
  return links

def get_question_from_link(link):
  time.sleep(random.uniform(0.5, 1.5)) # random delay to avoid being blocked
  try:
    response = requests.get(link)
    response.raise_for_status()
    question_text = extract_question_text(response.text)
    if question_text:
      return question_text
  except Exception as e:
      print(f"Error crawling URL {link}: {e}")
  return None

def crawl_links(links):
    """
    Crawls the given links and prints their HTTP status.
    """
    # Get number of CPU cores, leaving one free for system processes
    nprocs = max(1, multiprocessing.cpu_count() - 1)
    
    # Create a pool of workers
    with Pool(processes=nprocs) as pool:
        # Use tqdm to show progress while mapping get_question_from_link across links
        questions = list(tqdm.tqdm(
            pool.imap(get_question_from_link, links),
            total=len(links),
            desc=f"Crawling links using {nprocs} processes"
        ))
        # Filter out None values and return valid questions
        return questions

def extract_question_text(html_snippet):
    soup = BeautifulSoup(html_snippet, 'html.parser')
    div = soup.find('div', class_='qa-main-heading')
    if div is None:
      return None
    h1 = div.find('h1')
    if h1 is None:
      return None
    a = h1.find('a')
    if a is None:
      return None
    span = a.find('span', itemprop='name')
    if span is None:
      return None
    question_text = span.get_text()
    return question_text

def main():
    sitemap_index_url = "https://q.sa3dny.net/sitemap-index.xml"
    links = extract_index_from_url(sitemap_index_url)
    count = 0
    if links:
        print(f"Found {len(links)} URLs in the sitemap.")
        for link in links:
          urls = fetch_sitemap(link)
          chunks = [urls[i:i+50] for i in range(0, len(urls), 50)]
          for chunk in chunks:
            questions = crawl_links(chunk)
            count += 1
            
            # Create DataFrame from the chunks and questions
            df = pd.DataFrame({
                'link': chunk,
                'question': questions
            })
            
            # Save to CSV with tab separator and quoted text fields
            df.to_csv(f"q_sa3dny_net/questions_{count}.csv", 
                     index=False, 
                     sep='\t', 
                     quoting=csv.QUOTE_NONNUMERIC)
            print(f"Saved {len(questions)} questions to questions_{count}.csv")

if __name__ == "__main__":
    main()