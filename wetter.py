import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium", sql_output="pandas")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Laden der Daten von meteostat
    - https://dev.meteostat.net/data neu auch als parquet
    - https://dev.meteostat.net/parameters?g=hourly&d=1 Inhalte des Datensatzs
    """)
    return


@app.cell
def _():
    import marimo as mo
    import duckdb
    import pandas as pd

    import meteostat as ms
    import matplotlib.pyplot as plt

    return duckdb, mo, ms


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Abfrage der Stationen rund um Bremen
    """)
    return


@app.cell
def _(ms):
    POINT = ms.Point(53.07250, 8.81305, 113)  # Try with your location

    # Get nearby weather stations
    stations = ms.stations.nearby(POINT, limit=1000)

    print(stations)
    return


@app.cell
def _(duckdb):
    duck = duckdb.connect()
    return (duck,)


@app.cell
def _(duck):
    duck.sql("create or replace table stations as select * from stations")
    return


@app.cell
def _(duck):
    duck.sql("create or replace table daily as select * from read_parquet('https://data.meteostat.net/daily/2026.parquet')")
    return


@app.cell
def _(duck):
    duck.sql("from daily where station = '10224' limit 1000")
    return


@app.cell
def _(duck):
    duck.sql("from daily where station = '10224' limit 1000").df().plot(y=['temp','tmin', 'tmax'], x='date')
    return


@app.cell
def _(duck):
    duck.sql("from daily where station = '10224' limit 1000").df().plot(y=['prcp','snwd'], x='date')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Abgleich mit Radzählstationen
    """)
    return


@app.cell
def _(duck):
    duck.sql("create or replace table rad as select * from read_csv('https://vmz.bremen.de/radzaehler-api/?action=Values&apiFormat=csv&resolution=daily&startDate=2026-01-01&endDate=2026-12-31&stationId%5B%5D=100002930&cacheBust=1777924807280')")
    return


@app.cell
def _(duck):
    duck.sql("describe rad")
    return


@app.cell
def _(duck):
    duck.sql("from rad").df().plot(x= "Zeitpunkt ('Y-m-d H:i:s')", y="Radweg Kleine Weser")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
