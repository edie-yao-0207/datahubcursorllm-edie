# this view settings notebook is used to alter the view schema evolution
# for the non_govramp_customer_data catalog. This will allow the
# schema to evolve when upstream tables change, since these views
# should be a straight pass through of the upstream tables.
# the Databricks default settings type: COMPENSATION,
# will not allow the schema to evolve.
# https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-view

catalog = "non_govramp_customer_data"
spark.sql(f"USE CATALOG {catalog}")

schemas = [row[0] for row in spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()]

for schema in schemas:
    views = spark.sql(f"SHOW VIEWS IN {catalog}.{schema}").collect()
    for view in views:
        if view.viewName.startswith("_") or schema == "information_schema":
            continue
        full_view_name = f"{catalog}.{schema}.{view.viewName}"
        query = f"ALTER VIEW {full_view_name} WITH SCHEMA EVOLUTION"
        print(f"running query: {query}")
        spark.sql(query)
