# Codeforces Data Analyzer

This project is a tool for analyzing Codeforces data, including user statistics, problem details, contest insights, and programming language trends. It uses a Supabase database to store scraped Codeforces data and provides a graphical user interface (GUI) for interaction and analysis.

## Features

### **User Authentication and Personalized Insights**
- Login with your Codeforces handle.
- View attempted problems and contest history.
- Identify contests authored by you.

### **Data Exploration and Analysis**
- Browse problem sets by tag.
- Analyze performance metrics of top programmers:
  - Longest streak of consecutive active days.
  - Total problems solved.
  - Contest scores (Div. 1 and Div. 2).
  - Participation frequency relative to registration duration.
- Discover top organizations based on user ratings.
- Highlight top-performing users from the American University in Cairo (AUC).
- Identify the most frequently attempted problems by Egyptian users.

## Technologies Used

- **Python 3.8+:** For scraping, data processing, and application logic.
- **Supabase:** Cloud-based database (PostgreSQL) and authentication services.
- **Scrapy, BeautifulSoup, or Selenium (Specify):** Web scraping libraries.
- **The GUI framework used in this project is PyQt5**

## Project Structure

```
codeforces-data-analyzer/
├── GUI/
│   ├── database.py          # Database connection setup
│   ├── GUI.py               # Main GUI application
│   └── queries.py           # SQL queries for data analysis
├── schema_design/
│   ├── entity_diagram.drawio # Entity-Relationship Diagram
│   └── SQL.sql               # Database schema file
├── web_scrapping_scripts/
│   ├── contest_scraper.py    # Contest data scraper
│   ├── contests.csv          # Contest data CSV
│   ├── problem_scraper.py    # Problem data scraper
│   ├── problem.csv           # Problem data CSV
│   ├── user_scraper.py       # User data scraper
│   ├── User.csv              # User data CSV
│   ├── populate_database.py  # Script to populate the database
│   ├── modify_csv_floatToInt.py # Data cleaning script
│   └── remove_duplicates.py  # Duplicate removal script
```

## Prerequisites

- Python 3.8+
- `pip` package manager
- Supabase account and project (credentials stored in a `.env` file — ensure it is excluded from the repository).

## Installation

1. **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/codeforces-data-analyzer.git
    cd codeforces-data-analyzer
    ```

2. **Set up a virtual environment:**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3. **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4. **Configure Supabase:**
    Follow Supabase documentation to set up your connection.

5. **Populate the database:**
    ```bash
    python web_scrapping_scripts/populate_database.py
    ```

## Running the Application

To start the GUI application:
```bash
python GUI/GUI.py
```


## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
