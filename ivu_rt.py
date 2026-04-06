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
    db = 'ivu_rt_superset.db' # aus dem Verzeichnis python/ivu/db
    duck = duckdb.connect(db, read_only=True)
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
def _(duck, mo, rt_red):
    _df = mo.sql(
        f"""
        select haltestelle_name.replace ('Ã¼', 'ü').replace ('Ã', 'ß').replace ('Ã¶', 'ö').replace('Ã¤', 'ä')
        from rt_red
            where haltestelle_name.replace ('Ã¼', 'ü').replace ('Ã', 'ß').replace ('Ã¶', 'ö').replace('Ã¤', 'ä') like '%Nordwohlde%'
        group by all
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Erstellen der Funktionen
    - df_res Erstellen des DataFrames für die ausgewählte Fahrt
    - chart_func Erstellung des Charts
    """)
    return


@app.cell
def _(duck):
    def df_res(fnr, start_datum, ende_datum):
        """Erstellt den Ergebnisdataframe für die ausgewählte Fahrt
        Rausfiltern besonders hoher Abweichungen und Begrenzung des Zeitbereichs
        """
        df = duck.sql(f"""
        select
        datum,
        kurs,
        nr,
        haltestelle_name,
        -- Ersetzen der Zeichen wegen unterschiedlicher Codierung der Haltestellennamen
        lpad(nr::TEXT, 2, '0') || ' ' || haltestelle_name.replace ('Ã¼', 'ü').replace ('Ã', 'ß').replace ('Ã¶', 'ö').replace('Ã¤', 'ä') as nr_name,
        abwab / 60 as ab_minute,
        abwan / 60 as an_minute
    from
        rt_red
    where
        kurs = {fnr}
        -- Bestimmen des Toleranzbereichs für Abweichungen nach oben und unten 
        and abwab < 1800
        and abwab > -120
        and abwan < 1800
        and abwan > -120
        -- Zeitbereich
        and datum >= '{start_datum}'
        and datum <= '{ende_datum}'
        """).df()
        return df


    return (df_res,)


@app.cell
def _(alt):
    def chart_func(df_in, x,y, title):
        chart = alt.Chart(
            df_in, 
            title=title, 
            width='container', 
            height=500
        ).mark_boxplot(
            extent=1.5, 
            box = {'color': 'lightblue'},
            median={'color': 'orangered'},
            outliers={'size':3, 'color':'darkgrey', 'fill':'darkgrey'},
            ticks={'color':'green', 'size':8}
        ).encode(
            alt.X(f"{x}:N").title('lfd. Nr. / Haltestelle'),
            alt.Y(f"{y}:Q").scale(zero=False).title('Abweichung Ankunft in Minuten'),  
        ).configure_title(
            fontWeight='normal', fontSize=12
        ).configure_axis(
            labelFontWeight='normal',
            titleFontSize = 14,
            titleFontWeight='normal', 
            labelFontSize = 12
        )
        return chart


    return (chart_func,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Erstellen der Fahrtliste und Ermitteln der plausiblen Fahrten
    """)
    return


@app.cell(hide_code=True)
def _(duck, ende_datum, mo, rt_red, start_datum):
    df_fahrtliste = mo.sql(
        f"""
        select
            linie,
            kurs,
            count(*) as anz_fahrten,
            min(anz_hst) as min_anz_hst,
            max(anz_hst) as max_anz_hst,
            max(anz_hst) - min(anz_hst) diff_anz_hst,
            min(datum) as von,
            max(datum) as bis,
            '<a href="chart_'||kurs::int||'.html">Link</a>' as link
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
        engine=duck
    )
    return (df_fahrtliste,)


@app.cell
def _(df_fahrtliste):
    # Filterung auf Fahrten 
    ## - mit einer gewissen Anzahl von Fahrten und 
    ## - gleicher Anzahl Haltestellen und 
    ## - mehr als zwei Haltestellen mit fehlender Angaben zu Abwwichung An/ab
    df_fahrten_sel = df_fahrtliste.query('(anz_fahrten > 5) and (abs(diff_anz_hst) == 0) and (min_anz_hst > 2)')
    return (df_fahrten_sel,)


@app.cell
def _(df_fahrten_sel):
    df_fahrten_sel.dtypes
    return


@app.cell
def _(df_fahrten_sel):
    df_fahrten_sel[['linie', 'kurs', 'von', 'bis', 'anz_fahrten', 'link']].style.format({"von": lambda t: t.strftime("%Y-%m-%d"), "bis": lambda t: t.strftime("%Y-%m-%d")}, precision=0).to_html('out/liste.html', index=False, escape=False)
    return


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
def _(df_fahrten_sel):
    df_fahrten_sel.to_excel('df_fahrten_sel.xlsx')
    return


@app.cell
def _(df_fahrten_sel):
    print(len(df_fahrten_sel))
    return


@app.cell(hide_code=True)
def _(duck, mo, rt_red, start_datum):
    _df = mo.sql(
        f"""
        select *
        from rt_red
        where kurs = 1131123 
        and datum >= '{start_datum}'
        """,
        engine=duck
    )
    return


@app.cell
def _(df_res, ende_datum, start_datum):
    df_res(fnr='6405426', start_datum=start_datum, ende_datum=ende_datum)
    return


@app.cell
def _():
    return


@app.cell
def _(chart_func, df_fahrten_sel, df_res, ende_datum, start_datum):
    for j, value in df_fahrten_sel[0:10000].iterrows():
        _fnr = int(value['kurs'])
        _df = df_res(fnr=_fnr, start_datum=start_datum, ende_datum=ende_datum)

        print(int(value['kurs']), _df.datum.min())

        _min_datum = _df.datum.min().date().strftime('%Y-%m-%d')
        _max_datum = _df.datum.max().date().strftime('%Y-%m-%d')    
        _anzahl = _df.datum.drop_duplicates().count()    
        _title = f"Fahrt {_fnr} von {_min_datum} bis {_max_datum} Anzahl {_anzahl}"
        _chart = chart_func(_df, 'nr_name', 'an_minute', _title )
        #_chart.save(f'out/chart_{_fnr}.pdf')
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
    _fnr = 1330011
    _df = df_res(fnr=_fnr, start_datum=start_datum, ende_datum=ende_datum)
    min_datum = _df.datum.min().date().strftime('%Y-%m-%d')
    max_datum = _df.datum.max().date().strftime('%Y-%m-%d')    
    anzahl = _df.datum.drop_duplicates().count() 
    title = f"Fahrt {_fnr} von {min_datum} bis {max_datum} Anzahl {anzahl}"

    _chart = chart_func(_df, 'nr_name', 'an_minute', title)
    _chart
    _chart.save(f'out/chart_{_fnr}.pdf')
    _chart.save(f'out/chart_{_fnr}.html')
    chart_intern = _chart
    return (chart_intern,)


@app.cell
def _(chart_intern):
    chart_intern
    return


@app.cell
def _(duck):
    duck.close()
    return


if __name__ == "__main__":
    app.run()
