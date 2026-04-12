import marimo

__generated_with = "0.23.1"
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
    from openpyxl.styles import NamedStyle

    return alt, duckdb, mo, pd


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
def _(duckdb):
    db = 'ivu_rt_superset.db' # aus dem Verzeichnis python/ivu/db
    duck = duckdb.connect(db, read_only=False)
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
        select
            count(*) as anzahl_zeilen,
            count(*) filter (abwan not null) an_zeilen_ohne_null, 
            count(distinct linie) as anzahl_linien,
            min(datum).strftime('%Y-%m-%d') as beginn,
            max(datum).strftime('%Y-%m-%d') as ende
        from
            rt_red
        group by all
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
            haltestelle_name.replace('Ã¼', 'ü').replace ('Ã', 'ß').replace ('Ã¶', 'ö').replace ('Ã¤', 'ä').replace('ß¤', 'ä') as hst
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
        lpad(nr::TEXT, 2, '0') || ' ' || haltestelle_name.replace ('Ã¼', 'ü').replace ('Ã', 'ß').replace ('Ã¶', 'ö').replace('Ã¤', 'ä').replace('ß¤', 'ä') as nr_name,
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
def _(duck):
    def df_res_list(fnr):
        """Erstellt den Ergebnisdataframe für die ausgewählte Fahrt
        Rausfiltern besonders hoher Abweichungen und Begrenzung des Zeitbereichs über Liste der verkehrenden Tage
        """
        df = duck.sql(f"""
        select
            l.datum,
            l.kurs,
            l.linie,
            l.buendel,
            l.nr,
            l.haltestelle_name,
            l.sollabfahrt,
            l.sollab_ts,
            -- Ersetzen der Zeichen wegen unterschiedlicher Codierung der Haltestellennamen
            lpad(l.nr::TEXT, 2, '0') || ' ' || l.haltestelle_name.replace ('Ã¼', 'ü').replace ('Ã', 'ß').replace ('Ã¶', 'ö').replace ('Ã¤', 'ä').replace ('ß¤', 'ä') as nr_name,
            l.abwab / 60 as ab_minute,
            l.abwan / 60 as an_minute
        from
            rt_red l
            join tbl_fahrtliste s on s.kurs = l.kurs
            and l.datum in s.list_datum
        where
            l.kurs = {fnr}
            and abwab < 1800
            and abwab > -120
            and abwan < 1800
            and abwan > -120
        order by datum, kurs, nr
        """).df()
        return df

    return (df_res_list,)


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
def _(mo):
    mo.md(r"""
    ### Ermitteln der Häufigkeit einer Fahrt auch bei unterschiedlichen Anzahlen von Haltestellen
    """)
    return


@app.cell(hide_code=True)
def _(duck, ende_datum, mo, rt_red, start_datum):
    _df = mo.sql(
        f"""
        -- erstellen einer temporären Tabelle mit der Liste der Fahrten und auswertbaren Tagen
        create or replace temporary table tbl_fahrtliste as
        select
            * exclude (anz)
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
                    ) as anz_erhoben,
                    list_datum,
                    -- list_datum_sep,
                    -- list_string_agg(list_datum) as list_datum_str,
                    list_min(list_datum) as von,
                    list_max(list_datum) as bis,
                    '<a href="chart/chart_' || kurs::int || '.html">Link</a>' as link
                from
                    (
                        select
                            buendel,
                            linie,
                            kurs,
                            anz_hst,
                            count(*) as anz,
                            -- STRING_AGG(datum.strftime ('%Y-%m-%d')::text, ',') as list_datum_sep,
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
                                    count(*) filter(abwab not null) as ab_not_null,
                                from
                                    rt_red
                                where
                                    datum >= '{start_datum.value}'
                                    and datum <= '{ende_datum.value}'
                                    -- and linie = 6440
                                    -- and abwab < 1800
                                    -- and abwab > -120
                                    -- and abwan < 1800
                                    -- and abwan > -120
                                group by all
                                order by
                                    kurs,
                                    datum
                            ) as foo
                        where
                            ab_not_null > 2
                            and an_not_null > 2
                            and (ab_not_null / anz_hst) > 0.75
                            and (an_not_null / anz_hst) > 0.75
                        group by all
                    ) as foo2
                order by
                    kurs
            )
        WHERE
            -- Rausfiltern der Fahrt die am öftestesten erhoben wurde
            anz = anz_erhoben
            and anz >= 4
        order by
            kurs
        """,
        engine=duck
    )
    return


@app.cell
def _(df_res_list):
    df_res_list(fnr=1226002)
    return


@app.cell
def _(duck):
    # Filterung auf Fahrten 
    ## - mit einer gewissen Anzahl von Fahrten und 
    ## - gleicher Anzahl Haltestellen und 
    ## - mehr als zwei Haltestellen mit fehlender Angaben zu Abwwichung An/ab
    df_fahrten_sel = duck.sql("from tbl_fahrtliste").df()
    return (df_fahrten_sel,)


@app.cell(hide_code=True)
def _(duck, mo, rt_red, start_datum):
    _df = mo.sql(
        f"""
        from rt_red where kurs = 1012079 and datum >= '{start_datum.value}'
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Erstellen der Liste gesamt und je Bündel
    """)
    return


@app.cell
def _(df_fahrten_sel):
    df_fahrten_sel[['buendel','linie', 'kurs', 'von', 'bis', 'anz_erhoben', 'link']].style.format({"von": lambda t: t.strftime("%Y-%m-%d"), "bis": lambda t: t.strftime("%Y-%m-%d")}, precision=0).to_html('out/liste.html', index=False, escape=False)
    return


@app.cell(hide_code=True)
def _(duck, mo, tbl_fahrtliste):
    df_list_buendel = mo.sql(
        f"""
        select distinct
            buendel
        from
            tbl_fahrtliste
            where buendel not null
        order by
            buendel
        """,
        engine=duck
    )
    return (df_list_buendel,)


@app.cell
def _(df_fahrten_sel, df_list_buendel):
    for j, value in df_list_buendel.iterrows():
        _buendel = value['buendel']
        print(_buendel.replace(' ', '_').replace('ü', 'ue').lower())
        df_fahrten_sel[['buendel','linie', 'kurs', 'von', 'bis', 'anz_erhoben', 'link']].query(f"buendel == '{_buendel}'").style.format({"von": lambda t: t.strftime("%Y-%m-%d"), "bis": lambda t: t.strftime("%Y-%m-%d")}, precision=0).to_html(f"out/liste_{_buendel.replace(' ', '_').replace('ü', 'ue').lower()}.html", index=False, escape=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Batchdurchlauf
    """)
    return


@app.cell
def _(df_res_list, pd):
    # Ermitteln der Median Werte 
    _arr = []
    _fnr = 1226005
    _median_erste = df_res_list(fnr=_fnr).query("nr == 1")['ab_minute'].median()
    _vorletzte_hst = df_res_list(fnr=1226002).nr.max()-1
    _median_vorletzte = df_res_list(fnr=_fnr).query(f"nr == {_vorletzte_hst}")['an_minute'].median()
    _buendel = df_res_list(fnr=_fnr)['buendel'].drop_duplicates()[0]
    _linie = df_res_list(fnr=_fnr)['linie'].drop_duplicates()[0]
    _arr.append([_fnr,_buendel,_linie,_median_erste, _median_vorletzte])
    pd.DataFrame(_arr, columns=['fnr','buendel','linie','median_erste', 'median_vorletzte'])
    #df_res_list(fnr=1226002)
    return


@app.cell
def _(df_fahrten_sel):
    df_fahrten_sel.to_excel('df_fahrten_sel.xlsx')
    return


@app.cell
def _(df_fahrten_sel):
    print(len(df_fahrten_sel))
    return


@app.cell
def _(chart_func, df_fahrten_sel, df_res_list, pd):
    _arr = []

    for _k, _value in df_fahrten_sel[0:10].iterrows():
        _fnr = int(_value['kurs'])
        _df = df_res_list(fnr=_fnr)

        #print(int(_value['kurs']), _df.datum.min())

        _min_datum = _df.datum.min().date().strftime('%Y-%m-%d')
        _max_datum = _df.datum.max().date().strftime('%Y-%m-%d')    
        _anzahl = _df.datum.drop_duplicates().count()    
        _title = f"Fahrt {_fnr} von {_min_datum} bis {_max_datum} Anzahl {_anzahl}"
        #print(_fnr, _min_datum, _max_datum, _anzahl)
        _chart = chart_func(_df, 'nr_name', 'an_minute', _title )
        #_chart.save(f'out/chart/chart_{_fnr}.pdf')
        _chart.save(f'out/chart/chart_{_fnr}.html')

        # Ermitteln der Median Werte je Fahrt erste ab und vorletzte an

        _median_erste = _df.query("nr == 1")['ab_minute'].median()
        _sollab = _df.query("nr == 1")['sollab_ts'].min()
        vorletzte_hst = _df.nr.max()-1
        _median_vorletzte = _df.query(f"nr == {vorletzte_hst}")['an_minute'].median()
        _buendel = df_res_list(fnr=_fnr)['buendel'].drop_duplicates()[0]
        _linie = df_res_list(fnr=_fnr)['linie'].drop_duplicates()[0]
        _arr.append([_fnr,_buendel,_linie,_median_erste, _median_vorletzte, _min_datum, _max_datum, _anzahl, _sollab, _sollab.hour, _sollab.minute])

        print(_fnr, _min_datum, _max_datum, _anzahl, _sollab.hour, _sollab.minute)

    df_median = pd.DataFrame(_arr, columns=['fnr','buendel','linie','median_erste', 'median_vorletzte', 'min_datum', 'max_datum', 'anzahl', 'sollab', 'stunde', 'minute'])
    return (df_median,)


@app.cell
def _(df_median, ende_datum, pd, start_datum):
    prefix = f"reports/median_[{start_datum.value} - {ende_datum.value}]"

    with pd.ExcelWriter(f'{prefix}.xlsx', engine='openpyxl') as writer:    
        df_median.sort_values(['buendel', 'linie', 'fnr'], ascending=False).style.format(precision=1).to_excel(writer, sheet_name='median', index=False)
        workbook  = writer.book
        ws = writer.sheets['median']
        ws.auto_filter.ref = ws.dimensions
        ws.freeze_panes = 'A2'

        for row in ws.iter_rows(min_row=2, min_col=4, max_col=5):
            for _cell in row:
                _cell.number_format = "0.00" # Display to 2dp
        

    df_median.to_parquet(f'{prefix}.parquet')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Einzeldurchlauf
    - nur für einen Zeitraum von bis , hier müssen die Verläufe identisch sein
    """)
    return


@app.cell
def _(df_res, ende_datum, start_datum):
    df_res(fnr=6405426, start=start_datum.value, ende=ende_datum.value)
    return


@app.cell
def _(chart_func, df_res, ende_datum, start_datum):
    _fnr = 1670002
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
