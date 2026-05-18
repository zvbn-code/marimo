import marimo

__generated_with = "0.23.5"
app = marimo.App(width="medium", sql_output="pandas")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import pandas as pd

    return duckdb, mo, pd


@app.cell
def _(pd):
    kal_file = 'https://daten.zvbn.de/all_tg.csv'
    kal = pd.read_csv(kal_file, sep=';')
    return (kal,)


@app.cell
def _(kal):
    kal
    return


@app.cell
def _(pd):
    file_ivu_stat_export = 'ivu/20260507_stat_fahrt_typ_kalendertag_ol_west_aus_sec.xlsx'
    f_raw = pd.read_excel(file_ivu_stat_export , skipfooter = 1)
    #Bereinigen der Spaltenköpfe und Umstellung auf Kleinschreibung
    f_raw = f_raw.rename(columns=str.lower)
    f_raw.columns = f_raw.columns.str.replace(' ', '_')
    f_raw.columns = f_raw.columns.str.replace('-', '_')
    f_raw.linie_intern = f_raw.linie_intern.astype('str')
    f_raw.shape
    return


@app.cell
def _(duckdb):
    duck = duckdb.connect(database=':memory:', read_only=False)
    duck.sql("create or replace table f as select * exclude (fahrtstart, fahrtende) from f_raw")
    return (duck,)


@app.cell
def _(duck):
    duck.sql("create or replace table kal as select * from kal")
    # duck.sql("alter table kal add datum date")
    # duck.sql("update kal set datum = cast(strptime(days, '%Y-%m-%d') AS DATE) ")
    sql = """create or replace view f_vt as (
    select * 
    from f 
    join kal k on f.datum::date = k.datum)
    """
    duck.sql(sql)
    return


@app.cell(hide_code=True)
def _(duck, f_vt, mo):
    _df = mo.sql(
        f"""
        from f_vt
        """,
        engine=duck
    )
    return


@app.cell
def _(duck):
    _sql = """ pivot f_vt
    on fzgtyp
    using sum(länge/1000)

    group by liniennummer
    order by liniennummer"""
    duck.sql(_sql)
    return


@app.cell
def _(duck):
    _sql = """ pivot f_vt
    on tg2
    using sum(länge/1000)

    group by liniennummer
    order by liniennummer"""
    duck.sql(_sql)
    return


@app.cell
def _(duck):
    _sql = """ pivot f_vt
    on tg2
    using sum(dauer_sec/3600)

    group by liniennummer
    order by liniennummer"""
    duck.sql(_sql)
    return


@app.cell(hide_code=True)
def _(duck, mo):
    _df = mo.sql(
        f"""
        select columns(*) as AS "min_\\0" from f_vt
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(duck, mo):
    _df = mo.sql(
        f"""
        select l.liniennummer, columns(h.* exclude(liniennummer)) AS "min_\0", km_tg2.* exclude(liniennummer), km_fzgtyp.* exclude(liniennummer)
            from
        (select distinct f_vt.liniennummer,
            from f_vt) l

        join 
        (pivot f_vt
        on tg2
        using sum(dauer_sec/3600)

        group by liniennummer
        order by liniennummer) h on l.liniennummer = h.liniennummer

        join 
        (pivot f_vt
        on tg2
        using sum(länge/1000)

        group by liniennummer
        order by liniennummer) km_tg2 on l.liniennummer = km_tg2.liniennummer

        join 
        (pivot f_vt
        on fzgtyp
        using sum(länge/1000)

        group by liniennummer
        order by liniennummer) km_fzgtyp on l.liniennummer = km_fzgtyp.liniennummer

        order by liniennummer
        """,
        engine=duck
    )
    return


@app.cell
def _(duck):
    _sql = """
    select l.liniennummer, 
        columns(h.* exclude(liniennummer)) AS "h_\\0", 
        columns(km_tg2.* exclude(liniennummer)) AS "km_tg_\\0", 
        columns(km_fzgtyp.* exclude(liniennummer)) AS "km_fzg_\\0", 

        from
    (select distinct f_vt.liniennummer,
        from f_vt) l

    join 
    (pivot f_vt
    on tg2
    using sum(dauer_sec/3600)

    group by liniennummer
    order by liniennummer) h on l.liniennummer = h.liniennummer

    join 
    (pivot f_vt
    on tg2
    using sum(länge/1000)

    group by liniennummer
    order by liniennummer) km_tg2 on l.liniennummer = km_tg2.liniennummer

    join 
    (pivot f_vt
    on fzgtyp
    using sum(länge/1000)

    group by liniennummer
    order by liniennummer) km_fzgtyp on l.liniennummer = km_fzgtyp.liniennummer

    order by liniennummer

    """
    df_agg = duck.sql(_sql).df()
    return (df_agg,)


@app.cell
def _(df_agg):
    df_agg.to_excel('agg.xlsx', index=False)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
