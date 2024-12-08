from bs4 import BeautifulSoup
import logging
from typing import Dict, List, Optional
from utils import ScraperUtils, retry_on_failure

class SubmissionScraper:
    def __init__(self, max_pages_per_contest: int = 5):
        self.utils = ScraperUtils()
        self.submissions_data = []
        self.max_pages = max_pages_per_contest

    @retry_on_failure(max_retries=3)
    def scrape_contest_submissions(self, contest_id: str, url: str) -> List[Dict]:
        """Scrape submissions for a specific contest"""
        logging.info(f"Scraping submissions for contest {contest_id}")
        submissions = []
        page = 1

        while page <= self.max_pages:
            page_url = f"{url}/page/{page}"
            response = self.utils.make_request(page_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the submissions table
            submissions_table = soup.find('table', {'class': 'status-frame-datatable'})
            if not submissions_table:
                break

            page_submissions = self._parse_submissions_table(submissions_table, contest_id)
            submissions.extend(page_submissions)

            # Check if there's a next page
            if not self._has_next_page(soup):
                break

            page += 1

        logging.info(f"Scraped {len(submissions)} submissions for contest {contest_id}")
        return submissions

    def _parse_submissions_table(self, table: BeautifulSoup, contest_id: str) -> List[Dict]:
        """Parse the submissions table and extract submission information"""
        submissions = []
        rows = table.find_all('tr')[1:]  # Skip header row

        for row in rows:
            try:
                submission = self._extract_submission_info(row, contest_id)
                if submission:
                    submissions.append(submission)
            except Exception as e:
                logging.error(f"Error parsing submission row: {str(e)}")
                continue

        return submissions

    def _extract_submission_info(self, row: BeautifulSoup, contest_id: str) -> Optional[Dict]:
        """Extract submission information from a table row"""
        try:
            cols = row.find_all('td')
            if len(cols) < 8:  # Ensure we have all required columns
                return None

            return {
                'submission_id': cols[0].text.strip(),
                'contest_id': contest_id,
                'submission_time': self._parse_submission_time(cols[1].text.strip()),
                'problem_id': self._extract_problem_id(cols[3]),
                'language': cols[4].text.strip(),
                'verdict': self._extract_verdict(cols[5]),
                'time': self._extract_time(cols[6].text.strip()),
                'memory': self._extract_memory(cols[7].text.strip()),
                'username': self._extract_username(cols[2])
            }
        except Exception as e:
            logging.error(f"Error extracting submission info: {str(e)}")
            return None

    @staticmethod
    def _parse_submission_time(time_str: str) -> str:
        """Parse submission time to standard format"""
        return time_str  # You might want to standardize the format

    @staticmethod
    def _extract_problem_id(problem_cell: BeautifulSoup) -> str:
        """Extract problem ID from problem cell"""
        problem_link = problem_cell.find('a')
        return problem_link.text.strip() if problem_link else ""

    @staticmethod
    def _extract_verdict(verdict_cell: BeautifulSoup) -> str:
        """Extract verdict from verdict cell"""
        verdict_span = verdict_cell.find('span', {'class': 'verdict-accepted'})
        return verdict_span.text.strip() if verdict_span else verdict_cell.text.strip()

    @staticmethod
    def _extract_time(time_str: str) -> Optional[int]:
        """Extract execution time in milliseconds"""
        try:
            return int(''.join(filter(str.isdigit, time_str)))
        except ValueError:
            return None

    @staticmethod
    def _extract_memory(memory_str: str) -> Optional[int]:
        """Extract memory usage in KB"""
        try:
            return int(''.join(filter(str.isdigit, memory_str)))
        except ValueError:
            return None

    @staticmethod
    def _extract_username(user_cell: BeautifulSoup) -> str:
        """Extract username from user cell"""
        user_link = user_cell.find('a')
        return user_link.text.strip() if user_link else ""

    @staticmethod
    def _has_next_page(soup: BeautifulSoup) -> bool:
        """Check if there's a next page of submissions"""
        pagination = soup.find('div', {'class': 'pagination'})
        if not pagination:
            return False
        next_link = pagination.find('span', {'class': 'next'})
        return bool(next_link)

    def run_scraper(self, contest_ids: List[str]) -> None:
        """Main method to run the scraper"""
        try:
            for contest_id in contest_ids:
                submissions_url = f"https://codeforces.com/contest/{contest_id}/status"
                submissions = self.scrape_contest_submissions(contest_id, submissions_url)
                self.submissions_data.extend(submissions)

            self.utils.save_to_csv(self.submissions_data, 'submissions.csv')
            
        except Exception as e:
            logging.error(f"Error in submission scraping: {str(e)}")
            raise

if __name__ == "__main__":
    # For testing purposes
    scraper = SubmissionScraper(max_pages_per_contest=5)
    test_contest_ids = ["1234", "1235"]  # Replace with actual contest IDs
    scraper.run_scraper(test_contest_ids)