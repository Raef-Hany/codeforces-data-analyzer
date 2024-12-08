import requests
import time
import json
import csv
from datetime import datetime

class CodeforcesUserCrawler:
    def __init__(self, api_key=None, api_secret=None):
        self.base_url = "https://codeforces.com/api/"
        self.api_key = api_key
        self.api_secret = api_secret
        # Rate limiting - Codeforces has a limit of 2 requests per second
        self.request_delay = 0.5  # 500ms between requests
        
    def _make_request(self, method, params=None):
        """Make a request to Codeforces API with rate limiting"""
        url = f"{self.base_url}{method}"
        if params:
            # Convert all param values to strings
            params = {k: str(v).lower() if isinstance(v, bool) else str(v) 
                     for k, v in params.items()}
            
        try:
            time.sleep(self.request_delay)  # Rate limiting
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making request to {url}: {str(e)}")
            return None
        
    def get_users(self):
        """Get list of all non-retired users (both active and inactive)"""
        params = {
            'activeOnly': False,  # Get both active and inactive users
            'includeRetired': False  # Don't include retired users
        }
        
        response = self._make_request('user.ratedList', params)
        if response and response['status'] == 'OK':
            return response['result']
        return []
    
    def get_all_user_data(self):
        """Get all required user data from the API"""
        users = self.get_users()
        complete_user_data = []
        total_users = len(users)
        
        for i, user in enumerate(users, 1):
            try:
                # Get only the data available directly from the API
                user_data = {
                    'username': user.get('handle', ''),
                    'rating': user.get('rating', 0),
                    'rank': user.get('rank', ''),
                    'max_rating': user.get('maxRating', 0),
                    'contribution': user.get('contribution', 0),
                    'organization': user.get('organization', ''),
                    'friend_count': user.get('friendOfCount', 0),
                    'registration_date': datetime.fromtimestamp(
                        user.get('registrationTimeSeconds', 0)
                    ).strftime('%Y-%m-%d'),
                    'city': user.get('city', ''),
                    'country': user.get('country', '')
                }
                
                complete_user_data.append(user_data)
                
                # Print progress every 100 users
                if i % 100 == 0:
                    print(f"Processed {i}/{total_users} users ({(i/total_users*100):.1f}%)")
                
            except Exception as e:
                print(f"Error processing user {user.get('handle', 'unknown')}: {str(e)}")
                continue
        
        print(f"Completed processing all {total_users} users")
        return complete_user_data

def save_to_csv(data, filename='codeforces_users.csv'):
    """Save the collected data to a CSV file"""
    if not data:
        print("No data to save")
        return
    
    # Get fieldnames from the first dictionary in the data
    fieldnames = list(data[0].keys())
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving data to CSV: {str(e)}")

def save_to_json(data, filename='codeforces_users.json'):
    """Save the collected data to a JSON file"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# Usage example
if __name__ == "__main__":
    crawler = CodeforcesUserCrawler()
    user_data = crawler.get_all_user_data()
    
    # Save data in both formats
    save_to_csv(user_data)
    save_to_json(user_data)  # Keep JSON export as backup
    
    print(f"Total users saved: {len(user_data)}")