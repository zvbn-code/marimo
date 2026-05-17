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
    duck = duckdb.connect()
    return (duck,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Abfrage der issues (Massenabfrage)
    """)
    return


@app.cell
def _(duck):
    duck.sql("""
    create or replace table issues as 
    select 
    -- unnest(issues), 
    unnest(issues).id as id, 
    unnest(issues).project.id as project,
    unnest(issues).tracker.id as tracker,
    unnest(issues).status.id as status,
    unnest(issues).priority.id as priority,
    unnest(issues).author.id as author,
    unnest(issues).assigned_to.id as assigned_to,
    unnest(issues).subject as subject,
    unnest(issues).description as description,
    unnest(issues).start_date as start_date,
    unnest(issues).due_date as due_date,
    unnest(issues).done_ratio as done_ratio,
    unnest(issues).is_private as is_private,
    unnest(issues).estimated_hours as estimated_hours,
    unnest(issues).custom_fields as custom_fields,
    unnest(issues).created_on as created_on,
    unnest(issues).updated_on as updated_on,
    unnest(issues).closed_on as closed_on,
    unnest(issues).relations as relations,


    from read_json('json/issues_*.json')""")
    return


@app.cell
def _(duck):
    duck.sql("""
    insert into issues
    select 
    -- unnest(issues), 
    unnest(issues).id as id, 
    unnest(issues).project.id as project,
    unnest(issues).tracker.id as tracker,
    unnest(issues).status.id as status,
    unnest(issues).priority.id as priority,
    unnest(issues).author.id as author,
    unnest(issues).assigned_to.id as assigned_to,
    unnest(issues).subject as subject,
    unnest(issues).description as description,
    unnest(issues).start_date as start_date,
    unnest(issues).due_date as due_date,
    unnest(issues).done_ratio as done_ratio,
    unnest(issues).is_private as is_private,
    unnest(issues).estimated_hours as estimated_hours,
    unnest(issues).custom_fields as custom_fields,
    unnest(issues).created_on as created_on,
    unnest(issues).updated_on as updated_on,
    unnest(issues).closed_on as closed_on,
    unnest(issues).relations as relations

    from read_json('json/issues_*.json')""")
    return


@app.cell(hide_code=True)
def _(duck, issues, mo):
    _df = mo.sql(
        f"""
        from issues
        """,
        engine=duck
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Einzelissue
    """)
    return


@app.cell
def _(duck):
    duck.sql("""
    create or replace table issue as 
    select
    id as id,
    project.id as project,
    author.id as author,
    assigned_to.id as assigned_to,
    priority.id as priority,
    tracker.id as tracker, 
    * exclude (project, author, assigned_to, id, priority, tracker)

    from

    (
    select 
    unnest(issue),
    from read_json('json/issue_*.json')
    )

    """)
    return


@app.cell(hide_code=True)
def _(duck, issue, mo):
    _df = mo.sql(
        f"""
        from issue
        """,
        engine=duck
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
