# Python imports
from couchdb import Server, ServerError
from couchdb.http import ResourceNotFound
from dagster import get_dagster_logger
import numpy as np
import random

# Setting up logger
logger = get_dagster_logger()


class CouchDB:
    """
    A class for interacting with a CouchDB.

    Attributes:
        host (str): The hostname or URL of the CouchDB server.
        port (int): The port number of the CouchDB server..
        uname (str): The username for authentication.
        pwd (str): The password for authentication.
        server (couchdb.Server): The CouchDB server connection object.
        db (couchdb.Database): The CouchDB database object.
    """

    def __init__(self, host="http://localhost", port=5984, uname="dap", pwd="dap"):
        self.server = None
        self.db = None

        try:
            # Establish connection to the CouchDB server
            self.server = Server(f"{host}:{port}")
            self.server.resource.credentials = (uname, pwd)
            logger.info("CouchDB Connection Successful.")
        except ServerError as e:
            logger.error(f"Error While Connecting to CouchDB: {e}")

    def load_data(self, data, db_name):
        """
        Loads data into a specified CouchDB database.

        Args:
            data (dict): The data to be loaded into the database.
            db_name (str): The name of the database where the data will be loaded.

        Raises:
            ResourceNotFound: If the specified database does not exist.
            Exception: For other unexpected errors.
        """
        if self.server is None:
            logger.error("No Connection to CouchDB.")
            return

        try:
            # Create the database if it doesn't exist
            if db_name not in self.server:
                self.server.create(db_name)

            self.db = self.server[db_name]

            # Extract data and save documents to the database
            cols = [col["name"] for col in data["meta"]["view"]["columns"]]
            vals = data["data"]
            docs = [dict(zip(cols, val)) for val in vals]

            # Setting random seed (limiting data because of long execution time)
            seed_value = 42
            random.seed(seed_value)
            np.random.seed(seed_value)
            docs = random.sample(docs, 10000)

            for doc in docs:
                self.db.save(doc)

            logger.info(f"Data Load To {db_name} Successful.")

        except ResourceNotFound:
            logger.error(f"CouchDB: Database {db_name} not found.")

        except Exception as e:
            logger.error(f"Error While Data Load To {db_name}: {e}")

    def fetch_data(self, db_name):
        """
        Fetches data from a specified CouchDB database.

        Args:
            db_name (str): The name of the database from which to fetch data.

        Returns:
            list: A list of documents retrieved from the database.

        Raises:
            ResourceNotFound: If the specified database does not exist.
            Exception: For other unexpected errors.
        """
        if self.server is None:
            logger.error("No Connection to CouchDB.")
            return None

        try:
            self.db = self.server[db_name]
            db_data = [doc for doc in self.db.view("_all_docs", include_docs=True)]
            logger.info(f"CouchDB: Data Fetch From {db_name} Successful.")
            return db_data

        except ResourceNotFound:
            logger.error(f"Database {db_name} not found.")

        except Exception as e:
            logger.error(f"Error While Fetching Data From {db_name}: {e}")

    def close_connection(self):
        """
        Closes the connection to the CouchDB server.
        """
        self.server = None
        logger.info("CouchDB Connection Terminated.")
