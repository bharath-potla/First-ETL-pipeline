# Python imports
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from dagster import get_dagster_logger

# Setting up logger
logger = get_dagster_logger()


class PostgresDB:
    """
    A class to interact with PostgreDB.

    Attributes:
        host (str): The hostname or IP address of the database server.
        port (int): The port number of the database server.
        db (str): The name of the database.
        uname (str): The username for authentication.
        pwd (str): The password for authentication.
        engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
        connection (psycopg2.extensions.connection): The psycopg2 database connection object.
    """

    def __init__(
        self, host="localhost", port=5432, db="postgres", uname="dap", pwd="dap"
    ):
        """
        Initializes a new PostgresDB instance and establishes a connection to the PostgreSQL database.

        Args:
            host (str): The hostname or IP address of the database server.
            port (int): The port number of the database server.
            db (str): The name of the database.
            uname (str): The username for authentication.
            pwd (str): The password for authentication.
        """
        self.engine = None
        self.connection = None

        try:
            # Create SQLAlchemy engine and establish psycopg2 connection
            self.engine = create_engine(
                f"postgresql+psycopg2://{uname}:{pwd}@{host}:{port}/{db}"
            )
            self.connection = psycopg2.connect(
                host=host, port=port, database=db, user=uname, password=pwd
            )
            logger.info("PostgresDB Connection Successful.")

        except (Exception, psycopg2.Error) as e:
            logger.error(f"Error While Connecting To PostgresDB : {e}")

    def load_data(self, data, table_name):
        """
        Loads data from a DataFrame into a specified table in the PostgreSQL database.

        Args:
            data (pandas.DataFrame): The DataFrame containing the data to be loaded.
            table_name (str): The name of the table in the database where the data will be loaded.

        Raises:
            psycopg2.Error: If an error occurs during data loading.
            Exception: For other unexpected errors.
        """
        try:
            data.to_sql(
                name=table_name, con=self.engine, if_exists="replace", index=False
            )
            self.connection.commit()
            logger.info(f"PostgresDB: Data Load To {table_name} Successful.")

        except (psycopg2.Error, Exception) as e:
            logger.error(f"Error While Data Load To {table_name}: {e}")

    def fetch_data(self, table_name):
        """
        Fetches all data from a specified table in the PostgreSQL database and returns it as a DataFrame.

        Args:
            table_name (str): The name of the table from which data will be fetched.

        Returns:
            pandas.DataFrame: The DataFrame containing the fetched data.

        Raises:
            psycopg2.Error: If an error occurs during data fetching.
            Exception: For other unexpected errors.
        """
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.engine)
            logger.info(f"PostgresDB: Data Fetch From {table_name} Successful.")
            return df

        except (psycopg2.Error, Exception) as e:
            logger.error(f"Error While Data Fetch From {table_name}: {e}")

    def close_connection(self):
        """
        Closes the connection to the PostgreSQL database.

        Raises:
            psycopg2.Error: If an error occurs while closing the connection.
            Exception: For other unexpected errors.
        """
        try:
            self.connection.close()
            logger.info("PostgresDB Connection Terminated.")

        except (psycopg2.Error, Exception) as e:
            logger.error(f"Error While Closing Connection: {e}")
