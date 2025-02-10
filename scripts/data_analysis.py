# Python Imports
import pandas as pd
import numpy as np
from dagster import op, In, get_dagster_logger

# Custom Imports
from postgres_connector import PostgresDB
from analysis_utils import *

# Setting up logger
logger = get_dagster_logger()


@op(ins={"start": In(bool)})
def run_analysis(start):
    """
    Performing analysis and generating charts.

    Parameters:
           start (str): Start date for analysis (not currently used).

    Returns:
           None
    """
    # Connect to PostgresDB
    postgres_obj = PostgresDB()

    # Fetch Pre-processed NYC Restaurants Data from PostgresDB
    df1 = postgres_obj.fetch_data("nyc_restraunts_cleaned")
    # Fetch Pre-processed NYC Inspections Data from PostgresDB
    df2 = postgres_obj.fetch_data("nyc_inspection_cleaned")
    # Fetch Pre-processed LA Inspections Data from PostgresDB
    df3 = postgres_obj.fetch_data("la_inspection_cleaned")

    # Close connection from PostgresDB
    postgres_obj.close_connection()

    # Top 10 Most Frequent Restaurants in NYC
    name_counts = df1.name.value_counts()[:10].reset_index()
    bar_chart(name_counts, "name", "count", "Top 10 Most Frequent Restaurants")

    # Types Distribution by NYC Borough
    hist_chart(df1, "borough", "Open Restaurant Borough vs Type", hue="type")

    # Sidewalk Approval DIstribution by NYC Borough
    hist_chart(
        df1,
        "borough",
        "Open Restaurant Borough vs Sidewalk Seating Approval",
        hue="sidewalk_seating_approval",
    )

    # Roadway Approval DIstribution by NYC Borough
    hist_chart(
        df1,
        "borough",
        "Open Restaurant Borough vs Roadway Seating Approval",
        hue="roadway_seating_approval",
    )

    # Quarter-wise Inspection Counts in NYC
    quarter_counts = df2.quarter.value_counts().reset_index()
    pie_chart(quarter_counts, "quarter", "count", "NYC Inspection Quarter")

    # Quarter-wise Inspection Counts in LA
    quarter_counts = df3.quarter.value_counts().reset_index()
    pie_chart(quarter_counts, "quarter", "count", "LA Inspection Quarter")

    # Grade-wise top 5 Restaurants in NYC
    a_grades_nyc = df2[df2.grade == "A"]["name"].value_counts()[:5].reset_index()
    b_grades_nyc = df2[df2.grade == "B"]["name"].value_counts()[:5].reset_index()
    c_grades_nyc = df2[df2.grade == "C"]["name"].value_counts()[:5].reset_index()
    bar_chart(a_grades_nyc, "name", "count", "NYC Top 5 Restraunts with A Grade")
    bar_chart(b_grades_nyc, "name", "count", "NYC Top 5 Restraunts with B Grade")
    bar_chart(c_grades_nyc, "name", "count", "NYC Top 5 Restraunts with C Grade")

    # Grade-wise top 5 Restaurants in LA
    a_grades_la = df3[df3.grade == "A"]["name"].value_counts()[:5].reset_index()
    b_grades_la = df3[df3.grade == "B"]["name"].value_counts()[:5].reset_index()
    c_grades_la = df3[df3.grade == "C"]["name"].value_counts()[:5].reset_index()
    bar_chart(a_grades_la, "name", "count", "LA Top 5 Restraunts with A Grade")
    bar_chart(b_grades_la, "name", "count", "LA Top 5 Restraunts with B Grade")
    bar_chart(c_grades_la, "name", "count", "LA Top 5 Restraunts with C Grade")

    # NYC Open Inspections
    open_inspections = pd.merge(
        df1, df2.drop("borough", axis=1), on="name", how="inner"
    )

    # NYC Open Restaurants Grade Distribution by Borough
    grade_borrough_open_counts = (
        open_inspections.groupby(["borough", "grade"]).size().reset_index(name="count")
    )
    lat_lon = {
        "Bronx": (40.8466508, -73.8785937),
        "Brooklyn": (40.6526006, -73.9497211),
        "Manhattan": (40.7886553, -73.9603028),
        "Queens": (40.7135078, -73.8283132),
        "Staten Island": (40.5724274, -74.1452078),
    }
    grade_borrough_open_counts["latitude"] = grade_borrough_open_counts["borough"].map(
        lambda x: lat_lon[x][0]
    )
    grade_borrough_open_counts["longitude"] = grade_borrough_open_counts["borough"].map(
        lambda x: lat_lon[x][1]
    )
    map_chart(
        grade_borrough_open_counts[grade_borrough_open_counts.grade == "A"],
        "borough",
        "count",
        "NYC Open Restaurants A Grades by Borough",
    )
    map_chart(
        grade_borrough_open_counts[grade_borrough_open_counts.grade == "B"],
        "borough",
        "count",
        "NYC Open Restaurants B Grades by Borough",
    )
    map_chart(
        grade_borrough_open_counts[grade_borrough_open_counts.grade == "C"],
        "borough",
        "count",
        "NYC Open Restaurants C Grades by Borough",
    )

    # NYC Open Restaurants Grades by Type
    hist_chart(
        open_inspections, "grade", "NYC Open Restaurants Type vs Grade", hue="type"
    )

    # NYC Open Restaurants Grades by Approvals
    hist_chart(
        open_inspections,
        "grade",
        "NYC Open Restaurants Type vs Sidewalk Seating Approval",
        hue="sidewalk_seating_approval",
    )
    hist_chart(
        open_inspections,
        "grade",
        "NYC Open Restaurants Type vs Roadway Seating Approval",
        hue="roadway_seating_approval",
    )
    hist_chart(
        open_inspections,
        "grade",
        "NYC Open Restaurants Type vs Alcohol Permission",
        hue="alcohol_permission",
    )

    df_nyc = df2.copy()
    df_la = df3.copy()

    # Making sure data is of similar time period
    min_date = max(df_nyc.inspection_date.min(), df_la.inspection_date.min())
    max_date = min(df_nyc.inspection_date.max(), df_la.inspection_date.max())
    df_nyc = df_nyc[
        (df_nyc.inspection_date >= min_date) & (df_nyc.inspection_date <= max_date)
    ]
    df_la = df_la[
        (df_la.inspection_date >= min_date) & (df_la.inspection_date <= max_date)
    ]

    # Overall grade% comparision NYC vs LA
    nyc_grades = df_nyc.grade.value_counts().reset_index()
    la_grades = df_la.grade.value_counts().reset_index()
    nyc_grades["grade%"] = np.round(nyc_grades["count"] / len(df_nyc) * 100, 2)
    la_grades["grade%"] = np.round(la_grades["count"] / len(df_la) * 100, 2)
    nyc_grades["state"] = "nyc"
    la_grades["state"] = "la"
    grades_comp = pd.concat([nyc_grades, la_grades], axis=0)
    bar_chart(
        grades_comp, x="grade", y="grade%", title="Grade% NYC vs LA", color="state"
    )

    # A grade% comparision NYC vs LA
    nyc_A_grades_yearly = df_nyc[df_nyc.grade == "A"].year.value_counts().reset_index()
    la_A_grades_yearly = df_la[df_la.grade == "A"].year.value_counts().reset_index()
    nyc_A_grades_yearly["grade%"] = np.round(
        nyc_A_grades_yearly["count"] / len(df_nyc) * 100, 2
    )
    la_A_grades_yearly["grade%"] = np.round(
        la_A_grades_yearly["count"] / len(df_la) * 100, 2
    )
    nyc_A_grades_yearly["state"] = "nyc"
    la_A_grades_yearly["state"] = "la"
    A_grades_comp = pd.concat([nyc_A_grades_yearly, la_A_grades_yearly], axis=0)
    bar_chart(
        A_grades_comp,
        x="year",
        y="grade%",
        title="A Grade% NYC vs LA Yearly",
        color="state",
    )

    # B grade% comparision NYC vs LA
    nyc_B_grades_yearly = df_nyc[df_nyc.grade == "B"].year.value_counts().reset_index()
    la_B_grades_yearly = df_la[df_la.grade == "B"].year.value_counts().reset_index()
    nyc_B_grades_yearly["grade%"] = np.round(
        nyc_B_grades_yearly["count"] / len(df_nyc) * 100, 2
    )
    la_B_grades_yearly["grade%"] = np.round(
        la_B_grades_yearly["count"] / len(df_la) * 100, 2
    )
    nyc_B_grades_yearly["state"] = "nyc"
    la_B_grades_yearly["state"] = "la"
    B_grades_comp = pd.concat([nyc_B_grades_yearly, la_B_grades_yearly], axis=0)
    bar_chart(
        B_grades_comp,
        x="year",
        y="grade%",
        title="B Grade% NYC vs LA Yearly",
        color="state",
    )

    # C grade% comparision NYC vs LA
    nyc_C_grades_yearly = df_nyc[df_nyc.grade == "C"].year.value_counts().reset_index()
    la_C_grades_yearly = df_la[df_la.grade == "C"].year.value_counts().reset_index()
    nyc_C_grades_yearly["grade%"] = np.round(
        nyc_C_grades_yearly["count"] / len(df_nyc) * 100, 2
    )
    la_C_grades_yearly["grade%"] = np.round(
        la_C_grades_yearly["count"] / len(df_la) * 100, 2
    )
    nyc_C_grades_yearly["state"] = "nyc"
    la_C_grades_yearly["state"] = "la"
    C_grades_comp = pd.concat([nyc_C_grades_yearly, la_C_grades_yearly], axis=0)
    bar_chart(
        C_grades_comp,
        x="year",
        y="grade%",
        title="C Grade% NYC vs LA Yearly",
        color="state",
    )
