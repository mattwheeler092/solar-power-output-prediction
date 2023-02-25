import pymongo


class MongoDBCollection:
    def __init__(self,
                 username,
                 password,
                 ip_address,
                 database_name,
                 collection_name):
        '''
        Using ip_address, database_name, collection_name,
        initiate the instance's attributes including ip_address,
        database_name, collection_name, client, db and collection.

        For pymongo, see more details in the following.
        https://pymongo.readthedocs.io
        '''
        self.username = username
        self.password = password
        
        self.ip_address = ip_address
        self.database_name = database_name
        self.collection_name = collection_name

        self.client = pymongo.MongoClient(f"mongodb+srv://{username}:{password}@{ip_address}")
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]
        
        self.connection_string = f"mongodb+srv://{username}:{password}@{ip_address}/{database_name}.{collection_name}"

    def return_db(self):
        '''
        Return db which is the database in the client
        '''
        return self.db

    def return_collection(self):
        '''
        Return db which belongs to the db.
        '''
        return self.collection

    def return_num_docs(self, query):
        '''
        Return the number of documents satisfying the given query.
        '''
        return self.collection.count_documents(query)

    def drop_collection(self):
        '''
        Drop the collection
        '''
        return self.collection.drop()

    def find(self, query, projection):
        '''
        Return an iteratatable using query and projection.
        '''
        for item in self.collection.find(query, projection):
            yield item

    def insert_one(self, doc):
        '''
        Insert the given document
        '''
        self.collection.insert_one(doc)

    def insert_many(self, docs):
        '''
        Insert the given documents
        '''
        self.collection.insert_many(docs)

    def update_many(self, filter, update):
        '''
        Update documents satisfying filter with update.
        Both filter and update are dictionaries.
        '''
        self.collection.update_many(filter, update)

    def find_id_for_cad_number(self, cad_number):
        '''
        Return _id for the given cad_numer
        '''
        return self.find({"cad_number": f"{cad_number}"}, {"_id": 1})

    def remove_record_in_weather(self, cad_number, val):
        '''
        Remove items in the weather array where the "value" is val.
        '''
        return self.update_many({"cad_number": f"{cad_number}"},
                                {'$pull': {"weather": {"value": val}}})
