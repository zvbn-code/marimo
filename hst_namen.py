import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium", sql_output="pandas")


@app.cell
def _():
    import duckdb
    import re
    import pandas as pd
    import marimo as mo

    return duckdb, mo, pd, re


@app.cell
def _(duckdb):
    duck = duckdb.connect()
    duck.sql("""install spatial;
             load spatial;""")
    return (duck,)


@app.cell
def _(pd):
    pd.__version__
    return


@app.cell
def _(ABKUERZUNGEN, ZUSATZWÖRTER, re):
    def safe_substitute(text:str, replacements:str):
        # Muster unabhängig von Groß/Kleinschreibung ersetzen,
        # aber übrige Großschreibung im Ursprungs-Text erhalten!
        for muster, ersatz in replacements.items():
            text = re.sub(muster, lambda m: ersatz, text)
        return text

    def haltestelle_kuerzen_regex(name:str, max_len:int=20) -> str:
        # 1. Klammerausdrücke entfernen
        name = re.sub(r"\s*\([^)]*\)", "", name)
        # 2. Kürzungsmuster anwenden
        name = safe_substitute(name, ABKUERZUNGEN)
        # 3. Unerwünschte Wörter entfernen
        wort_liste = name.split()
        gekuerzt = [w for w in wort_liste if w.lower() not in [z.lower() for z in ZUSATZWÖRTER]]
        # 4. Doppelte oder zu viele Leerzeichen vermeiden
        kurztext = " ".join(gekuerzt)
        kurztext = re.sub(r"\s+", " ", kurztext).strip()
        # 5. Maximale Länge respektieren
        if len(kurztext) > max_len:
            kurztext = kurztext[:max_len-1] + "…"
        return kurztext  # <---- keine Anpassung mehr an der Groß/Kleinschreibung!


    return (haltestelle_kuerzen_regex,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Definition der Abkürzungen und Zusatzwörter
    """)
    return


@app.cell
def _():
    ABKUERZUNGEN = {
        r"(Straße|Strasse)\b": "Str.",    
        r"(straße|strasse)\b": "str.",    
        r"Platz\b": "Pl.",
        r"platz\b": "pl.",
        r"Hauptbahnhof\b": "Hbf",
        r"Bahnhof\b": "Bf",
        r"Haltestelle\b": "",
        r"Universität\b": "Uni",
        r"Zentraler Omnibusbahnhof\b": "ZOB",
        r"Osterholz-Scharmbeck\b": "OHZ",

        # usw.
    }

    ZUSATZWÖRTER = ["Haltestelle", "Hst"]
    return ABKUERZUNGEN, ZUSATZWÖRTER


@app.cell
def _():
    namen = [
        "Oldenburg(Oldg) Lappan",
        "Frankfurt(Main) Hauptbahnhof",
        "Gießen (Lahn) Haltestelle",
        "Heinrich-Heine Straße (Nord) Haltestelle",
        "Langemarckstraße",
        "Maximilianplatz Haltestelle"
    ]
    return (namen,)


@app.cell
def _(haltestelle_kuerzen_regex, namen):
    for name in namen:
        print(f"{name} → {haltestelle_kuerzen_regex(name, 40)}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Einlesen der Hafas Daten
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### bahnhof
    """)
    return


@app.cell
def _(duck, pd):
    # Define the column widths
    colspecs = [(0, 9), (9, 14), (14, 210)]
    names = ["nummer", "art", "name"]

    # Read the fixed-width file
    df = pd.read_fwf("hafas/bahnhof", colspecs=colspecs, names=names, skiprows=1, encoding='LATIN1')
    duck.sql("create or replace table bahnhof as select * from df")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### bfkoord
    """)
    return


@app.cell
def _(pd):
    # Define the column widths
    _colspecs = [(0, 9), (9, 21), (21, 32), (32,41), (41,250)]
    _names = ["nummer", "lon", "lat", "trenner", "name"]

    # Read the fixed-width file
    df_bfkoord = pd.read_fwf("hafas/bfkoord", colspecs=_colspecs, names=_names, skiprows=2, encoding='LATIN1')
    return (df_bfkoord,)


@app.cell(hide_code=True)
def _(df_bfkoord, duck, mo):
    _df = mo.sql(
        f"""
        create or replace table bfkoord as
        select
            *,
            st_makepoint (lon, lat) as geom
        from
            df_bfkoord
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Einlesen der VBN-Grenzen
    """)
    return


@app.cell(hide_code=True)
def _(duck, mo):
    _df = mo.sql(
        f"""
        create or replace table vbn as
        select
            *
        from
            st_read ('grenzen/vbn_01082018.shp') as vbn
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(duck, mo, vbn):
    _df = mo.sql(
        f"""
        select st_extent(geom) from vbn
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Auswertung
    """)
    return


@app.cell
def _(duck, haltestelle_kuerzen_regex):
    duck.create_function("haltestelle_kuerzen_regex", haltestelle_kuerzen_regex)
    return


@app.cell(hide_code=True)
def _(bahnhof, duck, mo):
    _df = mo.sql(
        f"""
        select distinct
            name.split ('$') [1] as teil1,
            length(name.split ('$') [1]) as laenge,
            haltestelle_kuerzen_regex (teil1, 60) as gekuerzt,
            length(gekuerzt) as laenge2,
            laenge - laenge2 as diff
        from
            bahnhof
        order by
            laenge desc
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(bfkoord, duck, mo, vbn):
    df_gekuerzt = mo.sql(
        f"""
        select
            bf.* exclude (nummer, trenner, lon, lat, geom),
            length(bf.name) as laenge,
                haltestelle_kuerzen_regex (bf.name, 60) as gekuerzt,
            length(gekuerzt) as laenge2,
            laenge - laenge2 as diff
        from
            bfkoord bf
            join vbn on st_dwithin (
                st_transform (bf.geom, 'EPSG:4326', 'EPSG:25832'),
                st_transform (vbn.geom, 'EPSG:4326', 'EPSG:25832'),
                20000
            )
        where
            bf.nummer < 800_000_000
        group by all
        order by
            laenge2 desc
        """,
        engine=duck
    )
    return (df_gekuerzt,)


@app.cell
def _(df_gekuerzt):
    df_gekuerzt.to_excel('gekuerzt.xlsx', index=False)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
