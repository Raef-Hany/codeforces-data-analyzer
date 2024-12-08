# GUI.py

from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from database import Database
from queries import Queries

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db = Database.get_instance()
        self.queries = Queries(self.db.client)
        self.user = None  # Logged-in user
        self.setup_ui()

    def setup_ui(self):
        self.setWindowTitle("Codeforces Analytics")
        self.setMinimumSize(1000, 800)

        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)

        # Login section
        self.create_login_section(layout)

        # Tabs for functionalities
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)
        self.tabs.setVisible(False)

        # Create tabs
        self.create_tabs()

    def create_login_section(self, layout):
        login_group = QGroupBox("Login")
        login_layout = QHBoxLayout()
        self.handle_input = QLineEdit()
        self.handle_input.setPlaceholderText("Enter Codeforces Handle")
        login_btn = QPushButton("Login")
        login_btn.clicked.connect(self.handle_login)
        login_layout.addWidget(self.handle_input)
        login_layout.addWidget(login_btn)
        login_group.setLayout(login_layout)
        layout.addWidget(login_group)

    def handle_login(self):
        screen_name = self.handle_input.text()
        user = self.queries.login_user(screen_name)
        if user:
            self.user = user
            QMessageBox.information(self, "Login Successful", f"Welcome {screen_name}!")
            self.populate_tabs()
            self.tabs.setVisible(True)
        else:
            QMessageBox.warning(self, "Login Failed", "User not found.")

    def create_tabs(self):
        # Tab 1: Contests as Writer
        self.tab_writer = QWidget()
        self.tabs.addTab(self.tab_writer, "Contests as Writer")

        # Tab 2: Top Users
        self.tab_top_users = QWidget()
        self.tabs.addTab(self.tab_top_users, "Top Users")

        # Tab 3: Top Organizations by User Ratings
        self.tab_top_orgs = QWidget()
        self.tabs.addTab(self.tab_top_orgs, "Top Organizations by Ratings")

        # Tab 4: Top AUC Users by Rating
        self.tab_top_auc = QWidget()
        self.tabs.addTab(self.tab_top_auc, "Top AUC Users")

    def populate_tabs(self):
        self.populate_writer_tab()
        self.populate_top_users_tab()
        self.populate_top_orgs_tab()
        self.populate_top_auc_tab()

    def populate_writer_tab(self):
        layout = QVBoxLayout()
        self.tab_writer.setLayout(layout)

        if self.user:
            # Get contests where user is a writer using username instead of userid
            contests = self.queries.get_user_written_contests(self.user['username'])
            if contests:
                for contest in contests:
                    layout.addWidget(QLabel(
                        f"Contest ID: {contest['contest_id']}, Name: {contest['contest_name']}, Date: {contest['start_time']}"
                    ))
            else:
                layout.addWidget(QLabel("No contests found where you are a writer."))
        else:
            layout.addWidget(QLabel("Please log in to view contests where you are a writer."))

    def populate_top_users_tab(self):
        layout = QVBoxLayout()
        self.tab_top_users.setLayout(layout)

        # Get top users by consecutive days and problems solved
        top_days, top_problems = self.queries.get_top_users_by_days_and_problems()

        top_days_group = QGroupBox("Top 10 Users by Max Consecutive Days")
        top_days_layout = QVBoxLayout()
        for user in top_days:
            top_days_layout.addWidget(QLabel(f"Username: {user['username']}, Max Consecutive Days: {user['max_streak']}"))
        top_days_group.setLayout(top_days_layout)
        layout.addWidget(top_days_group)

        top_problems_group = QGroupBox("Top 10 Users by Problems Solved")
        top_problems_layout = QVBoxLayout()
        for user in top_problems:
            top_problems_layout.addWidget(QLabel(f"Username: {user['username']}, Problems Solved: {user['problems_solved']}"))
        top_problems_group.setLayout(top_problems_layout)
        layout.addWidget(top_problems_group)

    def populate_top_orgs_tab(self):
        layout = QVBoxLayout()
        self.tab_top_orgs.setLayout(layout)

        # Create country selection section
        selection_group = QGroupBox("Select Country")
        selection_layout = QHBoxLayout()

        # Add country dropdown
        self.country_combo = QComboBox()
        # Get unique countries from database
        response = self.queries.client.table('User') \
            .select('country') \
            .not_.is_('country', 'null') \
            .execute()
        countries = sorted(list(set(u['country'] for u in response.data if u['country'])))
        self.country_combo.addItems(countries)

        # Add search button
        search_btn = QPushButton("Show Organizations")
        search_btn.clicked.connect(self.show_top_orgs)

        selection_layout.addWidget(QLabel("Country:"))
        selection_layout.addWidget(self.country_combo)
        selection_layout.addWidget(search_btn)
        selection_group.setLayout(selection_layout)
        layout.addWidget(selection_group)

        # Create results section
        self.orgs_results = QGroupBox("Top 5 Organizations")
        self.orgs_results_layout = QVBoxLayout()
        self.orgs_results.setLayout(self.orgs_results_layout)
        layout.addWidget(self.orgs_results)

    def show_top_orgs(self):
        # Clear previous results
        while self.orgs_results_layout.count():
            child = self.orgs_results_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()

        country = self.country_combo.currentText()
        if not country:
            return

        # Get top organizations
        top_orgs = self.queries.get_top_organizations_by_ratings(country)
        
        if top_orgs:
            self.orgs_results.setTitle(f"Top 5 Organizations in {country}")
            for org in top_orgs:
                label = QLabel(f"Organization: {org['organization']}\nAverage Rating: {org['avg_rating']:.2f}")
                self.orgs_results_layout.addWidget(label)
        else:
            self.orgs_results_layout.addWidget(QLabel("No organizations found for this country."))

    def populate_top_auc_tab(self):
        layout = QVBoxLayout()
        self.tab_top_auc.setLayout(layout)

        top_users = self.queries.get_top_auc_users()
        auc_group = QGroupBox("Top 10 AUC Users by Rating")
        auc_layout = QVBoxLayout()
        if top_users:
            for user in top_users:
                auc_layout.addWidget(QLabel(f"Username: {user['username']}, Rating: {user['rating']}"))
        else:
            auc_layout.addWidget(QLabel("No users from AUC found."))
        auc_group.setLayout(auc_layout)
        layout.addWidget(auc_group)

if __name__ == "__main__":
    import sys
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())