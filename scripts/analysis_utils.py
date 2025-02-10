# Python Imports
import plotly.express as px


def pie_chart(df, x, y, title):
    """
    Generate pie chart based on dataframe values.

    Args:
        df (pandas.DataFrame): Input dataframe containing chart data.
        x (str): Column name for labels.
        y (str): Column name for values.
        title (str): Title of the chart.
    """
    fig = px.pie(df, values=y, names=x, title=title)
    fig.show()


def bar_chart(df, x, y, title, color=None):
    """
    Generate bar chart based on dataframe values.

    Args:
        df (pandas.DataFrame): Input dataframe containing chart data.
        x (str): Column name for x-axis.
        y (str): Column name for y-axis.
        title (str): Title of the chart.
        color (str, optional): Column name for color encoding. Default is None.
    """
    if not color:
        fig = px.bar(df, x=x, y=y, title=title)
    else:
        fig = px.bar(df, x=x, y=y, title=title, color=color, barmode="group")
    fig.show()


def hist_chart(df, x, title, hue=None):
    """
    Generate histogram based on dataframe values.

    Args:
        df (pandas.DataFrame): Input dataframe containing chart data.
        x (str): Column name for histogram bins.
        title (str): Title of the chart.
        hue (str, optional): Column name for color encoding. Default is None.
    """
    if not hue:
        fig = px.histogram(df, x=x, title=title)
    else:
        fig = px.histogram(df, x=x, color=hue, barmode="group", title=title)
    fig.show()


def map_chart(df, x, y, title):
    """
    Generate map-based scatter plot using latitude and longitude.

    Args:
        df (pandas.DataFrame): Input dataframe containing location data.
        x (str): Column name for color encoding.
        y (str): Column name for marker size.
        title (str): Title of the chart.
    """
    px.set_mapbox_access_token("YOUR_MAPBOX_TOKEN")

    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        color=x,
        size=y,
        hover_name=x,
        zoom=9,
        mapbox_style="carto-darkmatter",
        title=title,
    )
    fig.show()
