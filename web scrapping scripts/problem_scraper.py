import requests
from bs4 import BeautifulSoup
import csv
import re
import time
import random
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import logging
import pandas as pd

class CodeforcesProblemScraper:
    def __init__(self, start_page=1, max_workers=None):
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        self.base_url = 'https://codeforces.com'
        self.start_page = start_page
        self.max_workers = max_workers or min(32, os.cpu_count() + 4)
        self.print_lock = Lock()
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('problem_scraper.log'),
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

    def extract_problem_data(self, row):
        try:
            # Parse Problem ID and Title
            cells = row.find_all('td')
            if len(cells) < 2:
                return None
                
            problem_id = cells[0].find('a').get_text(strip=True)
            title = cells[1].find('div').get_text(strip=True)
            
            # Parse Tags
            tags = []
            tags_elements = cells[1].find_all('a', class_='notice')
            for tag in tags_elements:
                tag_text = tag.get_text(strip=True)
                if tag_text:
                    tags.append(tag_text)
            
            # Parse Difficulty
            difficulty_cell = row.find('span', class_='ProblemRating')
            difficulty = difficulty_cell.get_text(strip=True) if difficulty_cell else ""

            return {
                'problem_id': problem_id,
                'title': title,
                'tags': ', '.join(tags),
                'difficulty': difficulty
            }
        except Exception as e:
            self.safe_print(f"Error extracting problem data: {e}")
            return None

    def process_page(self, page, csv_writer):
        self.safe_print(f"Processing problem set page {page}...")
        soup = self.fetch_page_data(f"{self.base_url}/problemset/page/{page}")
        if not soup:
            return False

        table = soup.find('table', class_='problems')
        if not table:
            return False

        rows = table.find_all('tr')[1:]
        for row in rows:
            problem_data = self.extract_problem_data(row)
            if problem_data:
                csv_writer.writerow(problem_data)
        
        return True

    def fetch_problems_parallel(self, output_file='problems_raw.csv'):
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['problem_id', 'title', 'tags', 'difficulty']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self.process_page, page, writer): page
                    for page in range(self.start_page, 99)  # Adjust max pages as needed
                }
                
                for future in as_completed(futures):
                    page = futures[future]
                    try:
                        future.result()
                        self.safe_print(f"Page {page} processed successfully.")
                    except Exception as e:
                        self.safe_print(f"Error processing page {page}: {e}")

def clean_duplicates(input_file='problems_raw.csv', output_file='problems_cleaned.csv'):
    df = pd.read_csv(input_file)
    df = df.replace('', pd.NA)
    df['problem_id'] = df['problem_id'].astype(str)
    df = df.drop_duplicates(subset=['problem_id'], keep='first')
    df_final = df.fillna('')
    df_final.to_csv(output_file, index=False)
    
    print(f"Original number of records: {len(df)}")
    print(f"Number of records after cleaning: {len(df_final)}")
    print(f"Removed {len(df) - len(df_final)} duplicate records")
    
    return df_final

def main():
    start_page = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    max_workers = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    scraper = CodeforcesProblemScraper(start_page=start_page, max_workers=max_workers)
    scraper.fetch_problems_parallel()

    clean_duplicates()

if __name__ == "__main__":
    main()