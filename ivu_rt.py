import marimo

__generated_with = "0.22.3"
app = marimo.App(width="medium", sql_output="pandas")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Auswerten der RBL-Aufzeichnungen aus IVU.control
    """)
    return


@app.cell
def _():
    import marimo as mo
    import duckdb
    import pandas as pd
    import altair as alt

    return alt, duckdb, mo


@app.cell
def _(mo):
    start_datum = mo.ui.date(label="Start Datum", value= '2025-10-01')
    ende_datum = mo.ui.date(label="Ende Datum", value= '2026-12-31')
    return ende_datum, start_datum


@app.cell
def _(ende_datum, mo, start_datum):
    mo.vstack([start_datum, ende_datum, mo.md(f"Zeitbereich Auswertung (inkl.): {start_datum.value} bis {ende_datum.value}")])
    return


@app.cell
def _():
    #start_datum = '2025-10-01'
    #ende_datum = '2026-12-31'
    return


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
def _(mo):
    mo.md(r"""
    ### Testen der Ersetzenfunktion wegen unterschiedlicher Codierung
    """)
    return


@app.cell(hide_code=True)
def _(duck, mo, rt_red):
    _df = mo.sql(
        f"""
        select
            haltestelle_name.replace ('Ã¼', 'ü').replace ('Ã', 'ß').replace ('Ã¶', 'ö').replace ('Ã¤', 'ä') as hst
        from
            rt_red
        where
            hst like '%Nordwohlde%'
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
    def df_res(fnr, start, ende):
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
            and datum >= '{start}'
            and datum <= '{ende}'
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
def _(duck, mo, rt_red):
    _df = mo.sql(
        f"""
        select
            min(datum),
            max(datum),
            linie
        from
            rt_red
        where
            linie::text like '%102%'
        group by ALL
        order by
            linie
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Ermitteln der Häufigkeit einer Fahrt auch bei unterschiedlichen Anzahlen von Haltestellen
    """)
    return


@app.cell(hide_code=True)
def _(duck, mo, rt_red):
    df_fahrtliste2 = mo.sql(
        f"""
        select
            *
        from
            (
                select
                    buendel,
                    linie,
                    anz_hst,
                    kurs,
                    anz,
                    sum(anz) over (
                        partition by
                            kurs
                    ) as ges_erhoben,
                    max(anz) over (
                        partition by
                            kurs
                    ) as meist_erhoben,
                    list_datum,
                from
                    (
                        select
                            buendel,
                            linie,
                            kurs,
                            anz_hst,
                            count(*) as anz,
                            list(datum) as list_datum
                        FROM
                            (
                                select
                                    datum,
                                    buendel,
                                    linie,
                                    kurs,
                                    count(*) as anz_hst,
                                    count(*) filter(abwan not null) as an_not_null,
                                    count(*) filter(abwab not null) as ab_not_null
                                from
                                    rt_red
                                where
                                    datum >= '2025-10-01'
                                    and datum <= '2025-11-12'
                                    -- and linie = 6440
                                group by all
                                order by
                                    kurs,
                                    datum
                            ) as foo
                        where
                            ab_not_null > 2
                            and an_not_null > 2
                        group by all
                    ) as foo2
                order by
                    kurs
            )
        WHERE
            anz = meist_erhoben
        order by
            kurs
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(duck, ende_datum, mo, rt_red, start_datum):
    df_fahrtliste = mo.sql(
        f"""
        select
            buendel,
            linie,
            kurs,
            count(*) as anz_fahrten,
            min(anz_hst) as min_anz_hst,
            max(anz_hst) as max_anz_hst,
            max(anz_hst) - min(anz_hst) diff_anz_hst,
            min(datum) as von,
            max(datum) as bis,
            '<a href="chart_' || kurs::int || '.html">Link</a>' as link
        from (select * from 
            (
                select
                    datum,
                    buendel,
                    linie,
                    kurs,
                    count(*) as anz_hst,
                    count(*) filter(abwan not null) as an_not_null,
                    count(*) filter(abwab not null) as ab_not_null
                from
                    rt_red
                where
                    datum >= '{start_datum.value}'
                    and datum <= '{ende_datum.value}'
                group by all
            )
        where
            -- Mindestanzahl der Abfahrten / Ankünfte mit Echtzeit
            an_not_null > 2
            and ab_not_null > 2) as foo


            group by all
        order by
            linie,
            kurs
        """,
        engine=duck
    )
    return (df_fahrtliste,)


@app.cell
def _(df_res):
    df_res(fnr=1226002, start='2025-11-12', ende='2025-11-12')
    return


@app.cell
def _(df_fahrtliste):
    df_fahrtliste.query("kurs == 1226002")
    return


@app.cell
def _(df_fahrtliste):
    # Filterung auf Fahrten 
    ## - mit einer gewissen Anzahl von Fahrten und 
    ## - gleicher Anzahl Haltestellen und 
    ## - mehr als zwei Haltestellen mit fehlender Angaben zu Abwwichung An/ab
    df_fahrten_sel = df_fahrtliste.query("(anz_fahrten > 5) and (abs(diff_anz_hst) == 0) and (min_anz_hst > 2) ")
    return (df_fahrten_sel,)


@app.cell
def _(df_fahrten_sel):
    df_fahrten_sel.dtypes
    return


@app.cell
def _(df_fahrten_sel):
    df_fahrten_sel[['buendel','linie', 'kurs', 'von', 'bis', 'anz_fahrten', 'link']].style.format({"von": lambda t: t.strftime("%Y-%m-%d"), "bis": lambda t: t.strftime("%Y-%m-%d")}, precision=0).to_html('out/liste.html', index=False, escape=False)
    return


@app.cell(hide_code=True)
def _(duck, mo, rt_red, start_datum):
    _df = mo.sql(
        f"""
        select * from rt_red 
            where kurs = 6405426
        and datum >= '{start_datum.value}'
        """,
        engine=duck
    )
    return


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
        where kurs = 1012078 
        and datum >= '{start_datum.value}'
        """,
        engine=duck
    )
    return


@app.cell
def _(df_res, ende_datum, start_datum):
    df_res(fnr=1012078 , start=start_datum.value, ende=ende_datum.value)
    return


@app.cell
def _(chart_func, df_fahrten_sel, df_res, ende_datum, start_datum):
    for j, value in df_fahrten_sel[0:10].iterrows():
        _fnr = int(value['kurs'])
        _df = df_res(fnr=_fnr, start=start_datum.value, ende=ende_datum.value)

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
    _fnr = 1102031
    _df = df_res(fnr=_fnr, start=start_datum.value, ende=ende_datum.value)
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
