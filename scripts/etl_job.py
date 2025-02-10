# Python Imports
from dagster import job

# Custom Imports
from data_ingestion import *
from data_preprocessing import *
from data_analysis import *


@job
def etl():
    # Running Visualizations
    run_analysis(
        # Loading Pre-processed Data into PostgresDB
        loading_cleaned_data(
            # Pre-processing NYC Restaurants JSON Data
            preprocess_nyc_restaurant(
                # Ingest NYC Restaurants JSON Data
                ingest_nyc_restaurants()
            ),
            # Pre-processing NYC Inspection CSV Data
            preprocess_nyc_inspection(
                # Ingest NYC Inspection CSV Data
                ingest_nyc_inspection()
            ),
            # Pre-processing LA Inspection JSON Data
            preprocess_la_inspection(
                # Ingest LA Inspection JSON Data
                ingest_la_inspection()
            ),
        ),
    )
