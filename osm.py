import marimo

__generated_with = "0.22.3"
app = marimo.App(width="medium", sql_output="pandas")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Abfrage der OSM pdf mittels read_osm
    - https://medium.com/data-science/how-to-read-osm-data-with-duckdb-ffeb15197390
    - https://duckdb.org/docs/current/core_extensions/spatial/functions#st_readosm
    """)
    return


@app.cell
def _():
    import marimo as mo
    import duckdb
    import pandas as pd

    return duckdb, mo


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Auswahl des Datensatzes
    """)
    return


@app.cell
def _():
    #pbf = 'bremen-260402.osm.pbf'
    pbf = 'niedersachsen-260402.osm.pbf'
    #pbf = 'germany-latest.osm.pbf'
    return (pbf,)


@app.cell
def _(duckdb):
    duck = duckdb.connect('osm.db')
    return (duck,)


@app.cell(hide_code=True)
def _(duck, mo):
    _df = mo.sql(
        f"""
        install spatial;
        load spatial;
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Beschreibung des Datensatzes
    """)
    return


@app.cell(hide_code=True)
def _(duck, mo, pbf):
    _df = mo.sql(
        f"""
        describe select *
             from st_readosm('{pbf}')
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Abfrage der Grenzen
    - https://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative#admin_level=*_Country_specific_values
    """)
    return


@app.cell(hide_code=True)
def _(duck, mo, pbf):
    _df = mo.sql(
        f"""
        select
            tags['boundary'] as boundary,
            tags['name'] as name,
            tags['admin_level'] as admin_level,
            tags['historic'],

            *
        from
            st_readosm ('{pbf}')
        where tags['boundary'] is not null
        and tags['admin_level'] in ('4', '6')
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Abfrage aller Daten mit Name *Schule*
    """)
    return


@app.cell(hide_code=True)
def _(duck, mo, pbf):
    _df = mo.sql(
        f"""
        select
            tags['amenity'] as amenity,
            tags['name'] as name,
            tags['operator'] as operator,
            *
        from
            st_readosm ('{pbf}')
        where
            -- tags IS NOT NULL
            -- and list_contains(map_keys(tags), 'name')
            -- AND list_extract(map_extract(tags, 'name'), 1).lower () like '%schule%'
            (tags['name']).lower () like '%schule%'
        order by
            name.lower(), 
            kind,
            id
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(duck, mo, pbf):
    _df = mo.sql(
        f"""
        select
            tags['amenity'] as amenity,
            tags['name'] as name,
            tags['operator'] as operator, 
            *
        from
            st_readosm ('{pbf}')
        where
            tags['amenity'] = 'car_sharing'
            -- tags['amenity'] in ('school', 'cinema', 'church', 'car_sharing', 'restaurant', 'cafe','nightclub','pub','bar','biergarten')
        order by
            NAME
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
