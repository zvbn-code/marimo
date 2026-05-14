import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium", sql_output="pandas")


@app.cell
def _():
    import marimo as mo
    import duckdb

    return duckdb, mo


@app.cell
def _(duckdb):
    duck = duckdb.connect(database='lake.db', read_only=False)
    return (duck,)


@app.cell(hide_code=True)
def _(duck, mo):
    _df = mo.sql(
        f"""
        INSTALL ducklake;
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(duck, mo):
    _df = mo.sql(
        f"""
        ATTACH 'ducklake:metadata.ducklake'
            AS my_ducklake
            (DATA_PATH 'lake/');

        USE my_ducklake;
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(duck, mo):
    _df = mo.sql(
        f"""

        show tables;
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(duck, mo):
    _df = mo.sql(
        f"""
        create table his as select * from read_csv("https://daten.zvbn.de/his_akt.csv")
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(duck, mo):
    _df = mo.sql(
        f"""
        show tables
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(duck, his, mo):
    _df = mo.sql(
        f"""
        select * from his at (version=> 1)
        """,
        engine=duck
    )
    return


@app.cell
def _():
    return


@app.cell
def _(duck):
    duck.close()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
