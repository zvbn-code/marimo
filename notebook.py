import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium", sql_output="pandas")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # erster Test Marimo und DuckDB
    - https://duckdb.org/docs/stable/guides/python/marimo
    - Verknüpfen der Datenbank
    - unter vscode wird vorher uv benötigt siehe Anleitung zur Extension
    """)
    return


@app.cell
def _():
    import marimo as mo
    import duckdb
    import pandas as pd
    import matplotlib.pyplot as plt

    return duckdb, mo


@app.cell
def _(duckdb):
    duck = duckdb.connect('marimo.db')
    return (duck,)


@app.cell(hide_code=True)
def _(duck, mo):
    _df = mo.sql(
        f"""
        CREATE or REPLACE table his as SELECT * FROM READ_CSV('https://daten.zvbn.de/his_akt.csv', delim= ';');
        CREATE or REPLACE table kreise as SELECT * FROM read_csv('https://daten.zvbn.de/his/bezirke3.csv', delim = ';');
        CREATE or REPLACE table his as SELECT * FROM read_csv('https://daten.zvbn.de/his_akt.csv', delim = ';');
        CREATE or REPLACE table ausstattungen as SELECT * FROM read_csv('https://daten.zvbn.de/his/ausstattungen.csv', delim=';');
        """,
        engine=duck
    )
    return


@app.cell
def _(ausstattungen, duck, mo):
    _df = mo.sql(
        f"""
        describe ausstattungen
        """,
        engine=duck
    )
    return


@app.cell
def _(duck, his, mo):
    _df = mo.sql(
        f"""
        SELECT
            name,
            dhid, linien_nr_ri
        FROM
            his
        where
            linien_nr_ri like '%680 %'
        order by name
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Einfache Auswertung Bordhöhe über alle
    """)
    return


@app.cell(hide_code=True)
def _(duck, his, mo):
    hoehe = mo.sql(
        f"""
        select
            bordsteinhoehe as hoehe,
            count(*) as anzahl
        from
            his
        group by ALL
        """,
        engine=duck
    )
    return (hoehe,)


@app.cell
def _(hoehe):
    #fig,ax = plt.subplots(figsize=(10,10))
    #ax.bar(data=hoehe, y='anzahl', x='hoehe', height=0.5)
    hoehe.plot.bar(x='hoehe', y='anzahl')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Bordhöhe nach Stadt / Landkreis
    """)
    return


@app.cell(hide_code=True)
def _(duck, his, mo):
    _df = mo.sql(
        f"""
        describe his
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Anzahl ohne Bordhöhe je Landkreis Stadt
    """)
    return


@app.cell(hide_code=True)
def _(duck, his, kreise, mo):
    _df = mo.sql(
        f"""
        select
            kreis,
            hoehe,
            case when hoehe < 18 then 'kl18' else 'other' end, 

            anzahl,
            SUM(anzahl) over (
                partition by
                    kreis
            ) count_hoehe,
            round(anzahl / count_hoehe, 3) as anteil_hoehe
        from
            (
                select
                    k.name as kreis,
                    h.bordsteinhoehe as hoehe,
                    count(*) as anzahl,
                from
                    his h
                    join kreise k on k.nummer = h.kreis_nr
                group by ALL
                order by
                    k.name,
                    h.bordsteinhoehe
            ) as foo
        order by
            hoehe,
            kreis
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(duck, his, kreise, mo):
    _df = mo.sql(
        f"""
        select
            k.name as kreis,
            h.bordsteinhoehe as hoehe,
            count(*) as anzahl,
        from
            his h
            join kreise k on k.nummer = h.kreis_nr
        where h.bordsteinhoehe is NULL
        group by ALL
        order by
            k.name
        """,
        engine=duck
    )
    return


@app.cell
def _(duck):
    duck.close()
    return


if __name__ == "__main__":
    app.run()
