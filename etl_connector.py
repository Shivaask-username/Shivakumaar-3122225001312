"""
Blocklist.de ETL Connector
Student Name: Shivakumaar
Roll Number: 3122225001312
Date: October 20, 2025
"""

import os
import requests
import logging
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
from typing import List, Dict, Optional
from time import sleep

# Load environment variables
load_dotenv()

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
DATABASE_NAME = os.getenv('DATABASE_NAME', 'security_data')
COLLECTION_NAME = os.getenv('COLLECTION_NAME', 'blocklist_raw')
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 30))
RATE_LIMIT_DELAY = int(os.getenv('RATE_LIMIT_DELAY', 2))

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BlocklistETLConnector:
    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DATABASE_NAME]
        self.collection = self.db[COLLECTION_NAME]
        logger.info(f"Connected to MongoDB: {DATABASE_NAME}.{COLLECTION_NAME}")

    def extract(self, attack_type: str) -> Optional[List[str]]:
        """Fetch data from blocklist.de for a given attack type."""
        url = f"https://lists.blocklist.de/lists/{attack_type}.txt"
        try:
            logger.info(f"Extracting data from: {url}")
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            ip_list = [line.strip() for line in response.text.split('\n')
                       if line.strip() and not line.startswith('#')]
            logger.info(f"Extracted {len(ip_list)} IPs for {attack_type}")
            sleep(RATE_LIMIT_DELAY)
            return ip_list
        except Exception as e:
            logger.error(f"Failed to extract data: {e}")
            return None

    def transform(self, ip_list: List[str], attack_type: str) -> List[Dict]:
        """Transform raw IP list into MongoDB compatible documents."""
        transformed = []
        for ip in ip_list:
            doc = {
                "ip_address": ip,
                "attack_type": attack_type,
                "source": f"https://lists.blocklist.de/lists/{attack_type}.txt",
                "retrieved_at": datetime.utcnow(),
                "ingestion_timestamp": datetime.utcnow()
            }
            transformed.append(doc)
        logger.info(f"Transformed {len(transformed)} documents")
        return transformed

    def load(self, documents: List[Dict]) -> bool:
        """Insert documents into MongoDB collection."""
        if not documents:
            logger.warning("No documents to insert")
            return False
        try:
            result = self.collection.insert_many(documents)
            logger.info(f"Inserted {len(result.inserted_ids)} documents")
            return True
        except Exception as e:
            logger.error(f"Failed to load documents: {e}")
            return False

    def run_etl(self, attack_types: List[str]):
        """Run ETL for each attack type."""
        total_loaded = 0
        for attack_type in attack_types:
            ip_list = self.extract(attack_type)
            if ip_list:
                docs = self.transform(ip_list, attack_type)
                success = self.load(docs)
                if success:
                    total_loaded += len(docs)
        logger.info(f"ETL pipeline completed. Total records loaded: {total_loaded}")

    def get_statistics(self) -> Dict:
        """Return simple stats from MongoDB."""
        stats = {
            "total_records": self.collection.count_documents({}),
            "by_attack_type": {}
        }
        pipeline = [
            {"$group": {"_id": "$attack_type", "count": {"$sum": 1}}}
        ]
        for record in self.collection.aggregate(pipeline):
            stats["by_attack_type"][record['_id']] = record['count']
        return stats

    def close(self):
        self.client.close()
        logger.info("MongoDB connection closed.")


def main():
    try:
        connector = BlocklistETLConnector()
        attack_types_to_process = ['ssh', 'mail', 'apache', 'ftp']  # you can add more
        connector.run_etl(attack_types_to_process)
        stats = connector.get_statistics()
        logger.info(f"Database Statistics: {stats}")
        connector.close()
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()
