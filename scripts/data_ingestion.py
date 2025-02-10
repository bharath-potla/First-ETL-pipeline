# Python imports
import urllib.request
from urllib.error import URLError
import pandas as pd
import json
from dagster import op, Out, get_dagster_logger

# Custom imports
from postgres_connector import PostgresDB
from couch_connector import CouchDB
from mongo_connector import MongoDB

# Setting up logger
logger = get_dagster_logger()


@op(out=Out(bool))
def ingest_nyc_inspection():
    """
    Fetches NYC inspection data from a CSV URL and ingests it into a PostgreSQL database.

    Returns:
        bool: True if the operation was successful, False otherwise.
    """
    result = True
    try:
        URL = "https://data.cityofnewyork.us/api/views/43nn-pn8j/rows.csv?accessType=DOWNLOAD"

        try:
            # Read CSV data from URL
            data = pd.read_csv(URL)
            logger.info("NYC Inspection Fetch From URL Seccessful.")

            # Connect to PostgreSQL database and load data
            postgres_obj = PostgresDB()
            postgres_obj.load_data(data, "nyc_inspection")
            postgres_obj.close_connection()

        except URLError as e:
            logger.error(f"URL Error: {e}")
        except TimeoutError as e:
            logger.error(f"Connection Timeout Error: {e}")
        except ConnectionError as e:
            logger.error(f"Connection Error: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")

    except Exception as e:
        logger.error(f"Error: {e}")
        result = False

    return result


@op(out=Out(bool))
def ingest_la_inspection():
    """
    Fetches LA inspection data from a JSON URL and ingests it into a CouchDB database.

    Returns:
        bool: True if the operation was successful, False otherwise.
    """
    result = True
    try:
        URL = (
            "https://data.lacity.org/api/views/29fd-3paw/rows.json?accessType=DOWNLOAD"
        )

        try:
            # Read JSON data from URL
            with urllib.request.urlopen(URL) as response:
                response = response.read().decode("utf-8")
                data = json.loads(response)
            logger.info("LA Inspection Fetch From URL Seccessful.")

            # Connect to CouchDB and load data
            couch_obj = CouchDB()
            couch_obj.load_data(data, "la_inspection")
            couch_obj.close_connection()

        except URLError as e:
            logger.error(f"URL Error: {e}")
        except TimeoutError as e:
            logger.error(f"Connection Timeout Error: {e}")
        except ConnectionError as e:
            logger.error(f"Connection Error: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON Decode Error: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")

    except Exception as e:
        logger.error(f"Error: {e}")
        result = False

    return result


@op(out=Out(bool))
def ingest_nyc_restaurants():
    """
    Fetches NYC restaurants data from a JSON URL and ingests it into a MongoDB database.

    Returns:
        bool: True if the operation was successful, False otherwise.
    """
    result = True
    try:
        URL = "https://data.cityofnewyork.us/api/views/pitm-atqc/rows.json?accessType=DOWNLOAD"

        try:
            # Read JSON data from URL
            with urllib.request.urlopen(URL) as response:
                response = response.read().decode("utf-8")
                data = json.loads(response)
            logger.info("NYC Restaurants Fetch From URL Seccessful.")

            # Connect to MongoDB and load data
            mongo_obj = MongoDB()
            mongo_obj.load_data(data, "nyc_restaurants")
            mongo_obj.close_connection()

        except URLError as e:
            logger.error(f"URL Error: {e}")
        except TimeoutError as e:
            logger.error(f"Connection Timeout Error: {e}")
        except ConnectionError as e:
            logger.error(f"Connection Error: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON Decode Error: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")

    except Exception as e:
        logger.error(f"Error: {e}")
        result = False

    return result
