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


@app.cell
def _(duck):
    def df_res(fnr, start_datum, ende_datum):
        df = duck.sql(f"""

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

    
        """).df()
        return df
    

    return (df_res,)


@app.cell
def _(alt):
    def chart_func(df_in, x,y, title):
        chart = alt.Chart(df_in, title=title).mark_boxplot(extent=1.5, 
                                                box = {'color': 'lightblue'},
                                                median={'color': 'orangered'}).encode(
        alt.X(f"{x}:N").title('Haltestelle'),
        alt.Y(f"{y}:Q").scale(zero=False).title('Abweichung Ankunft in Minuten'),   

        ).configure_title(fontWeight='normal', fontSize=12)
        return chart
    

    return (chart_func,)


app._unparsable_cell(
    r"""
    select
        linie,
        kurs,
        count(*) as anz_fahrten,
        min(anz_hst) as min_anz_hst,
        max(anz_hst) as max_anz_hst,
        max(anz_hst) - min(anz_hst) diff_anz_hst
    from
        (
            select
                datum,
                linie,
                kurs,
                count(*) as anz_hst
            from
                rt_red
            where
                datum >= '{start_datum}'
                and datum <= '{ende_datum}'
            group by all
        ) as foo
    group by all
    order by
        linie,
        kurs
    """,
    column=None, disabled=False, hide_code=True, name="_"
)


@app.cell
def _(df_fahrtliste):
    # Filterung auf Fahrten mit einer gewissen Anzahl und gleicher Anzahl Haltestellen
    df_fahrten_sel = df_fahrtliste.query('(anz_fahrten > 5) and (abs(diff_anz_hst) == 0)')
    return (df_fahrten_sel,)


@app.cell
def _():
    start_datum = '2025-10-01'
    ende_datum = '2026-12-31'
    return ende_datum, start_datum


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Batchdurchlauf
    """)
    return


@app.cell
def _(chart_func, df_fahrten_sel, df_res, ende_datum, start_datum):
    for j, value in df_fahrten_sel[0:10].iterrows():
        print(int(value['kurs']))
        _fnr = int(value['kurs'])

        _df = df_res(fnr=_fnr, start_datum=start_datum, ende_datum=ende_datum)

        _min_datum = _df.datum.min().date().strftime('%Y-%m-%d')
        _max_datum = _df.datum.max().date().strftime('%Y-%m-%d')    
        _anzahl = _df.datum.drop_duplicates().count()    
        _title = f"Fahrt {_fnr} von {_min_datum} bis {_max_datum} Anzahl {_anzahl}"
        _chart = chart_func(_df, 'nr_name', 'an_minute', _title )
        _chart.save(f'out/chart_{_fnr}.pdf')
        _chart.save(f'out/chart_{_fnr}.html')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Einzeldurchlauf
    """)
    return


@app.cell
def _(chart_func, df_res, ende_datum, start_datum):
    _fnr = 1330001
    _df = df_res(fnr=_fnr, start_datum=start_datum, ende_datum=ende_datum)
    min_datum = _df.datum.min().date().strftime('%Y-%m-%d')
    max_datum = _df.datum.max().date().strftime('%Y-%m-%d')    
    anzahl = _df.datum.drop_duplicates().count() 
    title = f"Fahrt {_fnr} von {min_datum} bis {max_datum} Anzahl {anzahl}"

    _chart = chart_func(_df, 'nr_name', 'an_minute', title)
    _chart
    return


@app.cell
def _(duck):
    duck.close()
    return


if __name__ == "__main__":
    app.run()
