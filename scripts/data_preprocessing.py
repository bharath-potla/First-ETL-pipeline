# Python Imports
import pandas as pd
import numpy as np
from dagster import op, Out, In, get_dagster_logger
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type

# Custom Imports
from postgres_connector import PostgresDB
from mongo_connector import MongoDB
from couch_connector import CouchDB

# Setting up logger
logger = get_dagster_logger()

# Define Dagster pandas dataframe types
nyc_restaurant_df = create_dagster_pandas_dataframe_type(
    name="nyc_restaurant_df",
    columns=[
        PandasColumn.string_column(name="type", non_nullable=True),
        PandasColumn.string_column(name="name", non_nullable=True),
        PandasColumn.string_column(name="borough", non_nullable=True),
        PandasColumn.string_column(name="sidewalk_seating_approval", non_nullable=True),
        PandasColumn.string_column(name="roadway_seating_approval", non_nullable=True),
        PandasColumn.string_column(name="alcohol_permission", non_nullable=True),
    ],
)

nyc_inspection_df = create_dagster_pandas_dataframe_type(
    name="nyc_inspection_df",
    columns=[
        PandasColumn.string_column(name="name", non_nullable=True),
        PandasColumn.string_column(name="borough", non_nullable=True),
        PandasColumn.datetime_column(name="inspection_date", non_nullable=True),
        PandasColumn.string_column(name="grade", non_nullable=True),
        PandasColumn.numeric_column(name="month", non_nullable=True),
        PandasColumn.numeric_column(name="year", non_nullable=True),
        PandasColumn.numeric_column(name="quarter", non_nullable=True),
    ],
)

la_inspection_df = create_dagster_pandas_dataframe_type(
    name="la_inspection_df",
    columns=[
        PandasColumn.datetime_column(name="inspection_date", non_nullable=True),
        PandasColumn.string_column(name="name", non_nullable=True),
        PandasColumn.string_column(name="grade", non_nullable=True),
        PandasColumn.numeric_column(name="month", non_nullable=True),
        PandasColumn.numeric_column(name="year", non_nullable=True),
        PandasColumn.numeric_column(name="quarter", non_nullable=True),
    ],
)


@op(ins={"start": In(bool)}, out=Out(nyc_restaurant_df))
def preprocess_nyc_restaurant(start):
    """
    Fetches and preprocesses NYC restaurant data from MongoDB.

    Args:
        start (bool): Dummy input to trigger the operation.

    Returns:
        pandas.DataFrame: Processed NYC restaurant data.
    """
    # Connect to MongoDB
    mongo_obj = MongoDB()
    # Fetch JSON from MongoDB
    nyc_restaurants = mongo_obj.fetch_data("nyc_restaurants")
    # Close connection from MongoDB
    mongo_obj.close_connection()

    # Transforming JSON to Dataframe
    cols = [col["name"] for col in nyc_restaurants[0]["meta"]["view"]["columns"]]
    vals = nyc_restaurants[0]["data"]
    df = pd.DataFrame(vals, columns=cols)

    # Initial records count
    initial_records = len(df)

    # Dropping duplicates
    df = df.drop_duplicates()

    # Feature selection
    use_cols = [
        "Seating Interest (Sidewalk/Roadway/Both)",
        "Restaurant Name",
        "Borough",
        "Approved for Sidewalk Seating",
        "Approved for Roadway Seating",
        "Qualify Alcohol",
    ]
    df = df[use_cols]

    # Renaming columns
    df.columns = [
        "type",
        "name",
        "borough",
        "sidewalk_seating_approval",
        "roadway_seating_approval",
        "alcohol_permission",
    ]

    # Converting name to lowercase
    df["name"] = df["name"].str.lower()

    # Converting object to string
    df["type"] = df["type"].astype("string")
    df["name"] = df["name"].astype("string")
    df["borough"] = df["borough"].astype("string")
    df["sidewalk_seating_approval"] = df["sidewalk_seating_approval"].astype("string")
    df["roadway_seating_approval"] = df["roadway_seating_approval"].astype("string")
    df["alcohol_permission"] = df["alcohol_permission"].astype("string")

    # Logging
    logger.info(f"Records before cleaning - {initial_records}")
    logger.info(f"Records after cleaning - {len(df)}")
    logger.info(f"Data loss - {initial_records - len(df)}")
    logger.info(
        f"Data loss% - {np.round((initial_records - len(df))/initial_records * 100,4)}%"
    )
    logger.info("NYC Restautants JSON Preprocess Successful.")

    return df


@op(ins={"start": In(bool)}, out=Out(nyc_inspection_df))
def preprocess_nyc_inspection(start):
    """
    Fetches and preprocesses NYC inspection data from PostgresDB.

    Args:
        start (bool): Dummy input to trigger the operation.

    Returns:
        pandas.DataFrame: Processed NYC inspection data.
    """
    # Connect to PostgresDB
    postgres_obj = PostgresDB()
    # Fetch CSV from PostgresDB
    df = postgres_obj.fetch_data("nyc_inspection")
    # Close connection from PostgresDB
    postgres_obj.close_connection()

    # Initial records count
    initial_records = len(df)

    # Dropping duplicates
    df = df.drop_duplicates()

    # Feature selection
    use_cols = ["DBA", "BORO", "INSPECTION DATE", "GRADE"]
    df = df[use_cols]

    # Dropping rows with null values in GRADE
    df = df.dropna(subset=["GRADE"], how="all")

    # Filtering out grades A, B, C
    df = df[df.GRADE.isin(["A", "B", "C"])]

    # Converting INSPECTION DATE from string to datetime
    df["INSPECTION DATE"] = pd.to_datetime(df["INSPECTION DATE"])

    # Filtering out INSPECTION DATE
    df = df[df["INSPECTION DATE"].dt.year >= 2016]

    # Renaming columns
    df.columns = ["name", "borough", "inspection_date", "grade"]

    # Converting name to lowercase
    df["name"] = df["name"].str.lower()

    # Extracting month, year and quarter
    df["month"] = df["inspection_date"].dt.month
    df["year"] = df["inspection_date"].dt.year
    df["quarter"] = df["inspection_date"].dt.quarter

    # Converting object to string
    df["name"] = df["name"].astype("string")
    df["borough"] = df["borough"].astype("string")
    df["grade"] = df["grade"].astype("string")

    # Logging
    logger.info(f"Records before cleaning - {initial_records}")
    logger.info(f"Records after cleaning - {len(df)}")
    logger.info(f"Data loss - {initial_records - len(df)}")
    logger.info(
        f"Data loss% - {np.round((initial_records - len(df)) / initial_records * 100, 4)}%"
    )
    logger.info("NYC Inspections CSV Preprocess Successful.")

    return df


@op(ins={"start": In(bool)}, out=Out(la_inspection_df))
def preprocess_la_inspection(start):
    """
    Fetches and preprocesses LA inspection data from CouchDB.

    Args:
        start (bool): Dummy input to trigger the operation.

    Returns:
        pandas.DataFrame: Processed LA inspection data.
    """
    # Connect to CouchDB
    couch_obj = CouchDB()
    # Fetch JSON from CouchDB
    la_inspection = couch_obj.fetch_data("la_inspection")
    # Close connection from CouchDB
    couch_obj.close_connection()

    # Transform JSON into DataFrame
    temp = [row["doc"] for row in la_inspection]
    df = pd.DataFrame(temp)

    # Initial records count
    initial_records = len(df)

    # Dropping duplicates
    df = df.drop_duplicates()

    # Feature selection
    use_cols = ["activity_date", "facility_name", "grade"]
    df = df[use_cols]

    # Filtering out grades A, B, C
    df = df[df.grade.isin(["A", "B", "C"])]

    # Renaming columns
    df.columns = ["inspection_date", "name", "grade"]

    # Converting inspection_date from string to datetime
    df["inspection_date"] = pd.to_datetime(df["inspection_date"])

    # Converting name to lowercase
    df["name"] = df["name"].str.lower()

    # Extracting month, year and quarter
    df["month"] = df["inspection_date"].dt.month
    df["year"] = df["inspection_date"].dt.year
    df["quarter"] = df["inspection_date"].dt.quarter

    # Converting object to string
    df["name"] = df["name"].astype("string")
    df["grade"] = df["grade"].astype("string")

    # Logging
    logger.info(f"Records before cleaning - {initial_records}")
    logger.info(f"Records after cleaning - {len(df)}")
    logger.info(f"Data loss - {initial_records - len(df)}")
    logger.info(
        f"Data loss% - {np.round((initial_records - len(df))/initial_records * 100,4)}%"
    )
    logger.info("LA Inspections JSON Preprocess Successful.")

    return df


@op(
    ins={
        "nyc_restaurant_df": In(nyc_restaurant_df),
        "nyc_inspection_df": In(nyc_inspection_df),
        "la_inspection_df": In(la_inspection_df),
    },
    out=Out(bool),
)
def loading_cleaned_data(nyc_restaurant_df, nyc_inspection_df, la_inspection_df):
    """
    Loads cleaned dataframes into PostgreSQL database.

    Args:
        nyc_restaurant_df (pandas.DataFrame): Cleaned NYC restaurant data.
        nyc_inspection_df (pandas.DataFrame): Cleaned NYC inspection data.
        la_inspection_df (pandas.DataFrame): Cleaned LA inspection data.
\
    Returns:
        bool: True if data loading is successful, False otherwise.
    """
    result = True
    try:

        # Connect to PostgresDB
        postgres_obj = PostgresDB()

        # Load nyc_restraunts_cleaned to PostgresDB
        postgres_obj.load_data(nyc_restaurant_df, "nyc_restraunts_cleaned")

        # Load nyc_inspection_cleaned to PostgresDB
        postgres_obj.load_data(nyc_inspection_df, "nyc_inspection_cleaned")

        # Load la_inspection_cleaned to PostgresDB
        postgres_obj.load_data(la_inspection_df, "la_inspection_cleaned")

        # Close connection from PostgresDB
        postgres_obj.close_connection()

    except Exception as e:
        logger.error(f"Error : {e}")
        result = False

    return result
