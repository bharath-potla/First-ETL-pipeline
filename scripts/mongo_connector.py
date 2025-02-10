# Python imports
import pymongo
import pymongo.errors
from dagster import get_dagster_logger

# Setting up logger
logger = get_dagster_logger()


class MongoDB:
    """
    A class for interacting with a MongoDB server.

    Attributes:
        host (str): The hostname or IP address of the MongoDB server.
        port (int): The port number of the MongoDB server.
        db (str): The name of the MongoDB database.
        uname (str): The username for authentication.
        pwd (str): The password for authentication.
        client (pymongo.MongoClient): The MongoDB client connection object.
        db (pymongo.database.Database): The MongoDB database object.
    """

    def __init__(self, host="localhost", port=27017, db="dap", uname="dap", pwd="dap"):
        """
        Initializes a new MongoDB instance and establishes a connection to the MongoDB server.

        Args:
            host (str): The hostname or IP address of the MongoDB server.
            port (int): The port number of the MongoDB server.
            db (str): The name of the MongoDB database.
            uname (str): The username for authentication.
            pwd (str): The password for authentication.
        """
        self.client = None
        self.db = None

        try:
            # Establish connection to the MongoDB server
            self.client = pymongo.MongoClient(f"mongodb://{uname}:{pwd}@{host}:{port}")
            self.db = self.client[db]
            logger.info("MongoDB Connection Successful.")

        except (Exception, pymongo.errors.ConnectionFailure) as e:
            logger.error(f"Error While Connecting To MongoDB : {e}")

    def load_data(self, data, collection_name):
        """
        Loads data into a specified collection in the MongoDB database.

        Args:
            data (dict or list): The data to be loaded into the collection.
            collection_name (str): The name of the collection where the data will be loaded.

        Raises:
            pymongo.errors.BulkWriteError: If an error occurs during bulk write operation.
            Exception: For other unexpected errors.
        """
        if self.client is None or self.db is None:
            logger.error("No Connection To MongoDB.")
            return

        try:
            # Insert single document or multiple documents into the collection
            if isinstance(data, dict):
                self.db[collection_name].insert_one(data)
            else:
                self.db[collection_name].insert_many(data)

            logger.info(f"MongoDB: Data Load To {collection_name} Successful.")

        except (pymongo.errors.BulkWriteError, Exception) as e:
            logger.error(f"Error While Data Load To {collection_name}: {e}")

    def fetch_data(self, collection_name):
        """
        Fetches all data from a specified collection in the MongoDB database.

        Args:
            collection_name (str): The name of the collection from which data will be fetched.

        Returns:
            list: A list of documents retrieved from the collection.

        Raises:
            pymongo.errors.PyMongoError: If an error occurs during data fetching.
            Exception: For other unexpected errors.
        """
        if self.client is None or self.db is None:
            logger.error("No Connection To MongoDB.")
            return None

        try:
            # Fetch all documents from the collection
            collection = list(self.db[collection_name].find())
            logger.info(f"MongoDB: Data Fetch From {collection_name} Successful.")
            return collection

        except (pymongo.errors.PyMongoError, Exception) as e:
            logger.error(f"Error While Data Fetch From {collection_name}: {e}")

    def close_connection(self):
        """
        Closes the connection to the MongoDB server.
        """
        if self.client is not None:
            self.client.close()
            logger.info("MongoDB Connection Terminated.")
