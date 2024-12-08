import requests
from bs4 import BeautifulSoup
import csv
import re
import time
import random
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import OrderedDict
import pandas as pd
from queue import Queue
from threading import Lock
import logging

class CodeforcesScraper:
    def __init__(self, start_page=1, max_workers=None):
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        self.base_url = 'https://codeforces.com'
        self.start_page = start_page
        self.max_workers = max_workers or min(32, os.cpu_count() + 4)  # Python's recommendation for I/O bound tasks
        self.print_lock = Lock()
        self.data_lock = Lock()
        self.contest_queue = Queue()
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def safe_print(self, message):
        with self.print_lock:
            self.logger.info(message)

    def fetch_page_data(self, page_url, retries=3):
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(1.5, 2.5))
                response = self.session.get(page_url, headers=self.headers, timeout=15)
                response.raise_for_status()
                return BeautifulSoup(response.content, 'html.parser')
            except requests.RequestException as e:
                self.safe_print(f"Error fetching {page_url} (attempt {attempt + 1}/{retries}): {e}")
                if attempt == retries - 1:
                    return None
                time.sleep(5)
        return None

    def extract_contest_data(self, row):
        try:
            name_cell = row.find('td', class_=None)
            if not name_cell:
                return None
            
            contest_name = name_cell.get_text(strip=True)
            contest_link = name_cell.find('a')
            contest_id = ''
            if contest_link and 'href' in contest_link.attrs:
                contest_id = contest_link['href'].split('/')[-1]
                if not contest_id.isdigit():
                    contest_id = ''

            writers_cell = row.find_all('td')[1] if len(row.find_all('td')) > 1 else None
            writers = []
            if writers_cell:
                writer_links = writers_cell.find_all('a')
                writers = [w.get_text(strip=True) for w in writer_links]
            writers_str = ', '.join(writers)

            start_cell = row.find_all('td')[2] if len(row.find_all('td')) > 2 else None
            start_time = ''
            if start_cell:
                start_time = start_cell.get_text(strip=True)

            length_cell = row.find_all('td')[3] if len(row.find_all('td')) > 3 else None
            length = length_cell.get_text(strip=True) if length_cell else ''

            return {
                'contest_id': contest_id,
                'contest_name': contest_name,
                'writers': writers_str,
                'start_time': start_time,
                'length': length
            }
        except Exception as e:
            self.safe_print(f"Error extracting contest data: {e}")
            return None

    def process_page(self, page):
        self.safe_print(f"Processing page {page}...")
        soup = self.fetch_page_data(f"{self.base_url}/contests/page/{page}")
        if not soup:
            return [], False

        page_contests = []
        tables = soup.find_all('table', class_='')
        if not tables:
            return [], False

        for table in tables:
            rows = table.find_all('tr')[1:]
            for row in rows:
                contest_data = self.extract_contest_data(row)
                if contest_data:
                    page_contests.append(contest_data)

        return page_contests, bool(page_contests)

    def fetch_contests_parallel(self):
        all_contests = []
        futures = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            page = self.start_page
            active_pages = set()
            
            while page <= 50:  # Adjust max pages as needed
                futures.append(executor.submit(self.process_page, page))
                active_pages.add(page)
                page += 1

                # Process completed futures
                for future in as_completed(futures):
                    contests, has_contests = future.result()
                    if contests:
                        with self.data_lock:
                            all_contests.extend(contests)
                            self.safe_print(f"Added {len(contests)} contests from page")

        return all_contests

def clean_duplicates(input_file='contests.csv', output_file='contests_cleaned.csv'):
    """
    Clean duplicates from the contests CSV file using pandas for efficient processing.
    """
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Convert empty strings to NaN for better handling
    df = df.replace('', pd.NA)
    
    # Sort by contest_id (ensuring numeric sorting)
    df['contest_id'] = pd.to_numeric(df['contest_id'], errors='coerce')
    df = df.sort_values('contest_id', ascending=False)
    
    # Drop duplicates based on contest_id, keeping the first occurrence (latest data)
    df_cleaned = df.drop_duplicates(subset=['contest_id'], keep='first')
    
    # Drop duplicates based on contest_name for entries without contest_id
    df_no_id = df[df['contest_id'].isna()]
    df_no_id = df_no_id.drop_duplicates(subset=['contest_name'], keep='first')
    
    # Combine the datasets
    df_final = pd.concat([
        df_cleaned[df_cleaned['contest_id'].notna()],
        df_no_id
    ])
    
    # Sort by contest_id again
    df_final = df_final.sort_values('contest_id', ascending=False)
    
    # Replace NaN back to empty strings
    df_final = df_final.fillna('')
    
    # Save to new CSV file
    df_final.to_csv(output_file, index=False)
    
    # Print statistics
    print(f"Original number of records: {len(df)}")
    print(f"Number of records after cleaning: {len(df_final)}")
    print(f"Removed {len(df) - len(df_final)} duplicate records")
    
    return df_final

def main():
    # Parse command line arguments
    start_page = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    max_workers = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    # Initialize and run the scraper
    scraper = CodeforcesScraper(start_page=start_page, max_workers=max_workers)
    contests = scraper.fetch_contests_parallel()
    
    # Save raw data
    with open('contests_raw.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['contest_id', 'contest_name', 'writers', 'start_time', 'length']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(contests)
    
    # Clean duplicates
    clean_duplicates('contests_raw.csv', 'contests_cleaned.csv')

if __name__ == "__main__":
    main()