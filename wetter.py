import marimo

__generated_with = "0.23.5"
app = marimo.App(width="medium", sql_output="pandas")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Laden der Daten von meteostat
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
    POINT = ms.Point(53.07250, 8.81305)  # Try with your location

    # Get nearby weather stations
    stations = ms.stations.nearby(POINT, limit=10)

    stations[['name', 'distance']]
    return (stations,)


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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Abgleich mit Radzählstationen
    - Quelle [vmz.bremen.de ](https://vmz.bremen.de/rad/radzaehlstationen-abfrage)
    """)
    return


@app.cell
def _(duck):
    start='2026-01-01'
    ende = '2026-12-31'
    duck.sql(f"create or replace table rad as select * from read_csv('https://vmz.bremen.de/radzaehler-api/?action=Values&apiFormat=csv&resolution=daily&startDate={start}&endDate={ende}&stationId%5B%5D=100002930&cacheBust=1777924807280')")
    return


@app.cell
def _(duck):
    duck.sql("from rad").df().plot(x= "Zeitpunkt ('Y-m-d H:i:s')", y="Radweg Kleine Weser", figsize=(20,10))
    return


@app.cell
def _(stations):
    stations['name'].sort_values().to_dict()
    return


@app.cell
def _(stations):
    mo_dict = stations['name'].sort_values().to_dict()
    return (mo_dict,)


@app.cell
def _(mo, mo_dict):
    # With search functionality
    dropdown = mo.ui.dropdown(    
    options=mo_dict,    
    value="10224",    
    label="Wähle eine Station",    
    searchable=True,
    )
    return (dropdown,)


@app.cell
def _(dropdown):
    dropdown
    return


@app.cell
def _(dropdown, duck):
    duck.sql(f"from daily where station = '{dropdown.selected_key}' limit 1000").df().plot(y=['temp','tmin', 'tmax'], x='date', title=f'{dropdown.selected_key} {dropdown.value} Temperatur (min/max/Durchschnitt)', figsize=(20,10))
    return


@app.cell
def _(dropdown, duck):
    duck.sql(f"from daily where station = '{dropdown.selected_key}' limit 1000").df().plot(y=['prcp','snwd'], x='date', title=f'{dropdown.selected_key} {dropdown.value} Niederschlag (mm) und Schneehöhe (cm)', figsize=(20,10))
    return


@app.cell
def _(dropdown, duck):
    styles1 = ['bs-','ro-','y^-']
    styles2 = ['rs-','go-','b^-']
    ax = duck.sql(f"from daily where station = '{dropdown.selected_key}' limit 1000").df().plot(y='temp', x='date', title=f'{dropdown.selected_key} {dropdown.value} Temperatur (min/max/Durchschnitt)', figsize=(20,10), style=styles1)
    ax1 = ax.twinx()
    duck.sql("from rad").df().plot(x= "Zeitpunkt ('Y-m-d H:i:s')", y="Radweg Kleine Weser", figsize=(20,10), ax=ax1, style=styles2)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Ausgabe der Metadaten der Station
    """)
    return


@app.cell
def _(dropdown, ms):
    station = ms.stations.meta(f'{dropdown.selected_key}')  # LaGuardia Airport
    # Get station inventory
    inventory = ms.stations.inventory(f'{dropdown.selected_key}')

    print(f"Data available from {inventory.start} to {inventory.end}.")

    print(station)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
