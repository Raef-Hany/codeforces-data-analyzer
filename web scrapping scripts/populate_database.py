import pandas as pd
import mysql.connector
from datetime import datetime
import sys
import csv

def connect_to_db():
    return mysql.connector.connect(
        host="DESKTOP-LVC98F3",
        user="root",
        password="1234567",
        database="codeforces"
    )

def get_or_create_country_id(cursor, country_name):
    if not country_name or pd.isna(country_name):
        return None
    
    cursor.execute("SELECT country_id FROM country WHERE name = %s", (country_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    
    cursor.execute("INSERT INTO country (name) VALUES (%s)", (country_name,))
    return cursor.lastrowid

def get_or_create_organization_id(cursor, org_name):
    if not org_name or pd.isna(org_name):
        return None
    
    cursor.execute("SELECT organization_id FROM organization WHERE name = %s", (org_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    
    cursor.execute("INSERT INTO organization (name) VALUES (%s)", (org_name,))
    return cursor.lastrowid

def clean_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, str):
        value = value.strip("'\"")
        if value.lower() == 'null' or not value:
            return None
    return value

def safe_int_convert(value):
    try:
        if pd.isna(value):
            return None
        return int(float(str(value)))
    except (ValueError, TypeError):
        return None

def import_users(csv_path):
    # Read CSV file with explicit data types and handling mixed types
    df = pd.read_csv(
        csv_path, 
        escapechar='\\', 
        quotechar="'", 
        encoding='utf-8',
        low_memory=False,
        dtype={
            'username': str,
            'rating': 'Int64',
            'rank': str,
            'max_rating': 'Int64',
            'contribution': 'Int64',
            'organization': str,
            'friend_count': 'Int64',
            'registration_date': str,
            'city': str,
            'country': str,
            'max_streak': 'Int64',
            'problems_solved': 'Int64',
            'processed': str
        }
    )
    
    # Connect to database
    conn = connect_to_db()
    cursor = conn.cursor()
    
    total_rows = len(df)
    successful_imports = 0
    failed_imports = 0
    
    # Process each row
    for index, row in df.iterrows():
        try:
            # Clean and get values with explicit type conversion
            username = clean_value(row['username'])
            city = clean_value(row.get('city'))
            country = clean_value(row.get('country'))
            organization = clean_value(row.get('organization'))
            
            # Get or create foreign keys
            country_id = get_or_create_country_id(cursor, country)
            organization_id = get_or_create_organization_id(cursor, organization)
            
            # Parse registration date
            reg_date_str = clean_value(row.get('registration_date'))
            try:
                reg_date = datetime.strptime(reg_date_str, '%Y-%m-%d') if reg_date_str else None
            except ValueError:
                print(f"Invalid date format for user {username}: {reg_date_str}")
                reg_date = None
            
            # Prepare user data with safe integer conversion
            user_data = {
                'screen_name': username,
                'city': city,
                'country_id': country_id,
                'organization_id': organization_id,
                'contribution': safe_int_convert(row.get('contribution')),
                'friend_count': safe_int_convert(row.get('friend_count')),
                'registration_date': reg_date,
                'problems_solved': safe_int_convert(row.get('problems_solved')),
                'max_streak': safe_int_convert(row.get('max_streak')),
                'rating': safe_int_convert(row.get('rating')),
                'max_rating': safe_int_convert(row.get('max_rating'))
            }
            
            # Skip if no username (required field)
            if not user_data['screen_name']:
                print(f"Skipping row {index}: No username provided")
                failed_imports += 1
                continue
            
            # Insert or update user
            query = """
                INSERT INTO user (
                    screen_name, city, country_id, organization_id, contribution,
                    friend_count, registration_date, problems_solved, max_streak,
                    rating, max_rating
                ) VALUES (
                    %(screen_name)s, %(city)s, %(country_id)s, %(organization_id)s,
                    %(contribution)s, %(friend_count)s, %(registration_date)s,
                    %(problems_solved)s, %(max_streak)s, %(rating)s, %(max_rating)s
                ) ON DUPLICATE KEY UPDATE
                    city = VALUES(city),
                    country_id = VALUES(country_id),
                    organization_id = VALUES(organization_id),
                    contribution = VALUES(contribution),
                    friend_count = VALUES(friend_count),
                    registration_date = VALUES(registration_date),
                    problems_solved = VALUES(problems_solved),
                    max_streak = VALUES(max_streak),
                    rating = VALUES(rating),
                    max_rating = VALUES(max_rating)
            """
            
            cursor.execute(query, user_data)
            successful_imports += 1
            
            # Print progress every 100 rows
            if index % 100 == 0:
                progress = (index + 1) / total_rows * 100
                print(f"Progress: {progress:.1f}% ({index + 1}/{total_rows} rows)")
            
        except Exception as e:
            print(f"Error processing user {username if 'username' in locals() else 'unknown'} at row {index}: {str(e)}")
            failed_imports += 1
            continue
    
    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()
    
    print("\nImport Summary:")
    print(f"Total rows processed: {total_rows}")
    print(f"Successful imports: {successful_imports}")
    print(f"Failed imports: {failed_imports}")
    print(f"Success rate: {(successful_imports/total_rows)*100:.1f}%")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <path_to_csv>")
        sys.exit(1)
    
    import_users(sys.argv[1])