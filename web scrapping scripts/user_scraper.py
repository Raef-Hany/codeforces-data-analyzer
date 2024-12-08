import pandas as pd
import concurrent.futures
import logging
import time
import random
import re
import cloudscraper
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import math
import requests
import gc
from memory_profiler import profile

class CodeforcesProfileScraper:
    def __init__(self, input_file='codeforces_users.csv', retry_count=3, backoff_factor=0.5, batch_size=50):
        self.input_file = input_file
        self.retry_count = retry_count
        self.backoff_factor = backoff_factor
        self.batch_size = batch_size
        self.base_url = "https://codeforces.com/profile/"

        # Setup logging
        logging.basicConfig(
            level=logging.WARNING,  # Set to WARNING to reduce log output
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('profile_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

        # Initialize cloudscraper with browser-like settings
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )

        self.ua = UserAgent()

    def add_dynamic_delay(self):
        """Add a delay between requests to avoid overwhelming the server."""
        delay = random.uniform(1.5, 4)  # Delay between 1.5 to 4 seconds
        self.logger.debug(f"Applying delay of {delay:.2f} seconds.")
        time.sleep(delay)

    def _extract_max_streak(self, soup):
        """Extract max streak from the user's profile page."""
        try:
            text = soup.get_text(separator=' ')
            match = re.search(r'(\d+)\s+days\s+in\s+a\s+row\s+max\.', text)
            if match:
                max_streak = int(match.group(1))
                return max_streak
        except Exception as e:
            self.logger.error(f"Error extracting max streak: {e}")
        return None

    def _extract_problems_solved(self, soup):
        """Extract total problems solved from the user's profile page."""
        try:
            text = soup.get_text(separator=' ')
            match = re.search(r'(\d+)\s+problems\s+solved\s+for\s+all\s+time', text)
            if match:
                problems_solved = int(match.group(1))
                return problems_solved
        except Exception as e:
            self.logger.error(f"Error extracting problems solved: {e}")
        return None

    def scrape_profile(self, username):
        """Scrape max_streak and problems_solved for a single user."""
        try:
            url = f"{self.base_url}{username}"

            for attempt in range(self.retry_count):
                try:
                    self.add_dynamic_delay()

                    headers = {
                        'User-Agent': self.ua.random
                    }

                    response = self.scraper.get(url, headers=headers, timeout=10)
                    response.raise_for_status()

                    soup = BeautifulSoup(response.text, 'html.parser')

                    # Extract data
                    max_streak = self._extract_max_streak(soup)
                    problems_solved = self._extract_problems_solved(soup)

                    # Explicitly delete large objects
                    del response
                    del soup

                    if max_streak is not None or problems_solved is not None:
                        return {
                            'username': username,
                            'max_streak': max_streak,
                            'problems_solved': problems_solved
                        }
                    else:
                        raise Exception("Failed to extract required data")

                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 403:
                        self.logger.error(f"Received 403 Forbidden for {username}. Backing off.")
                        time.sleep(60)  # Wait longer before retrying
                    else:
                        self.logger.error(f"HTTP Error for {username}: {e}")

                    if attempt >= self.retry_count - 1:
                        raise
                except Exception as e:
                    self.logger.error(f"Error on attempt {attempt + 1} for {username}: {str(e)}")
                    # Exponential backoff
                    sleep_time = math.pow(2, attempt)
                    self.logger.info(f"Sleeping for {sleep_time} seconds before retrying.")
                    time.sleep(sleep_time)

                    if attempt >= self.retry_count - 1:
                        raise

        except Exception as e:
            self.logger.error(f"Error scraping profile for {username}: {e}")
            return None

    @profile
    def update_users_data(self):
        """Update existing CSV with max_streak and problems_solved."""
        try:
            # Read existing CSV
            df = pd.read_csv(self.input_file)

            # Ensure necessary columns exist
            if 'max_streak' not in df.columns:
                df['max_streak'] = pd.Series(dtype='Int64')
            else:
                df['max_streak'] = df['max_streak'].astype('Int64')

            if 'problems_solved' not in df.columns:
                df['problems_solved'] = pd.Series(dtype='Int64')
            else:
                df['problems_solved'] = df['problems_solved'].astype('Int64')

            if 'processed' not in df.columns:
                df['processed'] = False

            total_users = len(df)
            self.logger.info(f"Processing {total_users} users")

            # Filter out already processed users
            users_to_process = df[~df['processed']]['username'].tolist()

            # Process users in batches
            for start in range(0, len(users_to_process), self.batch_size):
                end = start + self.batch_size
                batch = users_to_process[start:end]
                self.logger.info(f"Processing batch {start // self.batch_size + 1} with {len(batch)} users")

                with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
                    future_to_username = {executor.submit(self.scrape_profile, username): username for username in batch}

                    for i, future in enumerate(concurrent.futures.as_completed(future_to_username), 1):
                        username = future_to_username.pop(future)
                        try:
                            result = future.result()
                            if result:
                                # Update DataFrame with new data
                                mask = df['username'] == result['username']
                                df.loc[mask, 'max_streak'] = result['max_streak'] if result['max_streak'] is not None else pd.NA
                                df.loc[mask, 'problems_solved'] = result['problems_solved'] if result['problems_solved'] is not None else pd.NA
                                df.loc[mask, 'processed'] = True

                        except Exception as e:
                            self.logger.error(f"Error processing user {username}: {e}")

                # Save progress after each batch
                df.to_csv(self.input_file, index=False)
                self.logger.info(f"Completed batch {start // self.batch_size + 1}")
                gc.collect()  # Explicit garbage collection
            
            self.logger.info("Successfully updated all user profiles")

        except Exception as e:
            self.logger.error(f"Error updating users data: {e}")

if __name__ == "__main__":
    try:
        scraper = CodeforcesProfileScraper(
            input_file='codeforces_users.csv',
            retry_count=3
        )
        scraper.update_users_data()
    except KeyboardInterrupt:
        print("\nScraping interrupted by user.")
    except Exception as e:
        print(f"An error occurred: {e}")