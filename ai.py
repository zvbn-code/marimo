import marimo

__generated_with = "0.23.2"
app = marimo.App(width="medium", sql_output="pandas")


@app.cell
def _():
    import marimo as mo
    import duckdb

    return (duckdb,)


@app.cell
def _(duckdb):
    duck = duckdb.connect()
    return (duck,)


@app.cell
def _(duck):
    df_his = duck.sql("select * from read_csv('https://daten.zvbn.de/his_akt.csv')").df()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
