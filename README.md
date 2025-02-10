# NYC & LA Restaurant Performance Analysis

This project analyzes restaurant performance in New York City and Los Angeles for the Database & Analytics Programming course at NCI, Dublin.

## Project Overview

This project aims to:

* **Data Ingestion:** Ingesting NYC open restaurants, NYC restaurant inspections and LA restaurant inspections data into respective databases.
* **Data Preprocessing:** Cleaning the data by handling null values, type casting, feature selection, etc.
* **Data Analysis:** Analyze the NYC open restaurants inspection performance and also compare the performace of restaurants in NYC with restaurants in LA.
* **Visualize insights:** Create data visualizations to effectively communicate findings.

## Technologies Used

* Programming Languages: Python, SQL
* Libraries/Frameworks:   - numpy, pandas, plotly, psycopg2, couchdb-python, pymongo, sqlalchemy, pendulum. dagster, dagster-pandas, dagit
* Databases: PostgreSQL, CouchDB, MongoDB

## Project Structure

* **data:** Contains raw data files.
* **scripts:** Houses code for data cleaning, analysis, and visualization.
* **plots:** Includes the plots generated during data analysis.
* **dependencies.yml:** File for creating conda environment with required packages.
* **docker-compose.yml:** File for setting up docker containers.
* **mongo-init.js:** File for initializing MongoDB.

## Getting Started

1. Pre-requisites: 
    Make sure the below requirements are installed in the system.<br>
        * Git<br>
        * Docker desktop<br>
        * Anaconda

2. Clone the repository:

    ```https://github.com/bharath-potla/First-ETL-pipeline.git```

3. Unzip DAP-ETL.zip.

4. Open terminal and navigate to DAP-ETL.

5. Run docker-compose:

    ```docker-compose up -d```

6. Create and activate anaconda environment:

    ```conda env create -f dependencies.yml```

    ```conda activate etl-env```

7. Navigate to inside scripts:

    ```cd scripts```

8. Trigger dagster ETL:

    ```dagit -f etl_job.py```


