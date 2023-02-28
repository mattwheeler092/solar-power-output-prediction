import os
import logging

from pymongo.errors import BulkWriteError, DuplicateKeyError
from pprint import pprint

import pymongo
from config import (
    MONGO_COLLECTION,
    MONGO_DB,
    MONGO_IP,
    MONGO_PASSWORD,
    MONGO_USERNAME,
)

logging.basicConfig(level=logging.WARN)

class MongoDB:
    """ Class to handle all connections and operations 
        related to the project MongoDB database. 
        Provides functionality to search the database 
        as well as insert / update database records """

    def __init__(self):
        """Initialised the connection to the mongoDB database"""
        self.client = pymongo.MongoClient(
            f"mongodb+srv://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_IP}"
        )
        self.db = self.client[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION]

    def find(self, query, projection):
        """Returns documents that match query. Doucments
        match projection structure"""
        for item in self.collection.find(query, projection):
            yield item

    def insert(self, docs):
        """Inserts many documents into db"""
        try:
            self.collection.insert_many(docs)
        except BulkWriteError as bwe:
            key = bwe.details['writeErrors'][0]['keyValue']['_id']
            logging.warn(f"INSERT KEY ERROR: {key}")

    def update(self, filter, update):
        """Updates documents that match filter query"""
        self.collection.update_many(filter, update)
