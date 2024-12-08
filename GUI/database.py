import os
from supabase import create_client, Client
from dotenv import load_dotenv

class Database:
    __instance = None

    @staticmethod
    def get_instance():
        if Database.__instance is None:
            Database()
        return Database.__instance

    def __init__(self):
        if Database.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            load_dotenv()
            url = os.getenv("SUPABASE_URL")
            key = os.getenv("SUPABASE_KEY")
            self.client = create_client(url, key)
            Database.__instance = self