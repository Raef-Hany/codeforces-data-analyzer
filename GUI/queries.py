# queries.py
from datetime import datetime

class Queries:
    def __init__(self, client):
        self.client = client

    def login_user(self, screen_name):
        # Login user by username
        response = self.client.table('User').select('*').eq('username', screen_name).execute()
        return response.data[0] if response.data else None

    def get_user_written_contests(self, username):
        """Get contests where user is writer"""
        print(f"Looking up contests for user: {username}")
        
        # Get user's ID from username string
        user_response = self.client.table('User') \
            .select('username, userid') \
            .eq('username', username) \
            .execute()
        
        print(f"User response: {user_response.data}")
        
        if not user_response.data:
            print("No user found")
            return []
        
        userid = user_response.data[0]['userid']
        print(f"Found userid: {userid}")
        
        # Get contest IDs where user is writer
        contests_response = self.client.table('contestwriter') \
            .select('*') \
            .eq('user_id', userid) \
            .execute()
        
        print(f"Contest writer response: {contests_response.data}")
        
        contest_ids = [c['contest_id'] for c in contests_response.data]
        
        if contest_ids:
            print(f"Found contest IDs: {contest_ids}")
            contests_info = self.client.table('contest') \
                .select('*') \
                .in_('contest_id', contest_ids) \
                .execute()
            print(f"Contest info: {contests_info.data}")
            return contests_info.data
        
        print("No contests found")
        return []

    def get_top_users_by_days_and_problems(self):
        # Get top 10 users by max_streak and problems_solved
        top_days_response = self.client.table('User') \
            .select('username, max_streak') \
            .order('max_streak', desc=True) \
            .limit(10).execute()

        top_problems_response = self.client.table('User') \
            .select('username, problems_solved') \
            .order('problems_solved', desc=True) \
            .limit(10).execute()

        return top_days_response.data, top_problems_response.data

    def get_top_auc_users(self):
        # Get top 10 users from 'The American University in Cairo' organization by rating
        response = self.client.table('User') \
            .select('username, rating') \
            .eq('organization', 'The American University in Cairo') \
            .order('rating', desc=True) \
            .limit(10).execute()
        
        # Filter out None ratings and format the data
        filtered_data = [
            user for user in response.data 
            if user['rating'] is not None
        ]
        
        return filtered_data

    def get_top_organizations_by_ratings(self, country):
        """Get top 5 organizations by ratings in specified country"""
        # Verify country exists in database
        print(f"Getting organizations for country: {country}")
        
        users_response = self.client.table('User') \
            .select('organization, rating') \
            .eq('country', country) \
            .execute()
        
        print(f"Found {len(users_response.data)} users in {country}")

        org_ratings = {}
        for user in users_response.data:
            org = user['organization']
            if not org:  # Skip users with no organization
                continue
            rating = user['rating'] or 0
            
            if org in org_ratings:
                org_ratings[org]['total_rating'] += rating
                org_ratings[org]['user_count'] += 1
            else:
                org_ratings[org] = {'total_rating': rating, 'user_count': 1}

        # Calculate average ratings
        avg_ratings = []
        for org, data in org_ratings.items():
            avg_rating = data['total_rating'] / data['user_count']
            avg_ratings.append({'organization': org, 'avg_rating': avg_rating})

        # Sort and get top 5 organizations
        top_orgs = sorted(avg_ratings, key=lambda x: x['avg_rating'], reverse=True)[:5]
        print(f"Found top organizations: {top_orgs}")
        return top_orgs

    # Remove or comment out methods that rely on missing tables
    # def get_user_activity(self, username):
    #     pass

    # def get_top_users_by_total_scores(self):
    #     pass

    # def get_top_users_by_participation_frequency(self):
    #     pass

    # def get_top_problems_by_egypt_users(self):
    #     pass