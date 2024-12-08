Codeforces Analytics System
This project provides a command-line interface to analyze Codeforces data including users, problems, contests, and programming statistics. The application connects to a Supabase database containing scraped Codeforces data.

Features
User login and activity tracking
View user's attempted problems and participated contests
View contests where user was writer
View top programmers by various metrics:
Days streak
Problems solved
Contest scores
Participation frequency
View top organizations by country
View top AUC users

Prerequisites
Python 3.8+
pip package manager
Access to Supabase database (credentials are in the .env file)

Create and activate virtual environment:

# Windows
python -m venv venv
venv\Scripts\activate

# Linux/MacOS
python3 -m venv venv
source venv/bin/activate

spiders/
├── database.py      # Database connection configuration
├── queries.py       # SQL query implementations
└── GUI.py         # Application entry point