from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
client = MongoClient(uri)

try:
    client.admin.command('ping')
    print("✅ MongoDB connection successful!")
    print(f"Databases: {client.list_database_names()}")
except Exception as e:
    print(f"❌ Connection failed: {e}")
finally:
    client.close()