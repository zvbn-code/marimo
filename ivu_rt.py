import marimo

__generated_with = "0.22.3"
app = marimo.App(width="medium", sql_output="pandas")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import pandas as pd
    import altair as alt

    return alt, duckdb, mo


@app.cell
def _(duckdb):
    duck = duckdb.connect('ivu_rt_superset.db', read_only=True)
    return (duck,)


@app.cell(hide_code=True)
def _(duck, mo, rt_red):
    _df = mo.sql(
        f"""
        describe rt_red
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(duck, ende_datum, fnr, mo, rt_red, start_datum):
    df_box = mo.sql(
        f"""
        select
            datum,
            kurs,
            nr,
            haltestelle_name,
            lpad(nr::TEXT, 2, '0') || ' ' || haltestelle_name.replace ('Ã¼', 'ü').replace ('Ã', 'ß').replace ('Ã¶', 'ö') as nr_name,
            abwab / 60 as ab_minute,
            abwan / 60 as an_minute
        from
            rt_red
        where
            kurs = {fnr}
            and abwab < 1800
            and abwab > -120
            and abwan < 1800
            and abwan > -120
            and datum >= '{start_datum}'
            and datum <= '{ende_datum}'
        """,
        output=False,
        engine=duck
    )
    return (df_box,)


@app.cell
def _(df_box, fnr):
    min_datum = df_box.datum.min().date().strftime('%Y-%m-%d')
    max_datum = df_box.datum.max().date().strftime('%Y-%m-%d')

    anzahl = df_box.datum.drop_duplicates().count()

    title = f"Fahrt {fnr} von {min_datum} bis {max_datum} Anzahl {anzahl}"
    return (title,)


@app.cell
def _():
    fnr = 1340011
    start_datum = '2025-10-01'
    ende_datum = '2026-12-31'
    return ende_datum, fnr, start_datum


@app.cell(hide_code=True)
def _(alt, df_box, title):
    alt.Chart(df_box, title=title).mark_boxplot(extent=1.5, 
                                                box = {'color': 'lightblue'},
                                                median={'color': 'orangered'}).encode(
        alt.X("nr_name:N"),
        alt.Y("an_minute:Q").scale(zero=False),   
  
    )
    return


@app.cell
def _(duck):
    duck.close()
    return


if __name__ == "__main__":
    app.run()
