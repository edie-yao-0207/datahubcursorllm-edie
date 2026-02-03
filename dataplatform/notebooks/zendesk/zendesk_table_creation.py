s3_location = "s3://samsara-zendesk/samsara_zendesk"
folders = dbutils.fs.ls(s3_location)

s3views = [
    "group",
    "organization",
    "ticket_custom_field",
    "ticket",
    "user",
    "ticket_field_history",
    "ticket_form_history",
    "ticket_tag",
    "ticket_comment",
]

for folder in folders:
    if "/" in folder.name and folder.name[0] != "_" and "test" not in folder.name:
        table_name = folder.path.replace(s3_location, "").replace("/", "")
        if table_name not in s3views:
            table_create_statement = f"CREATE OR REPLACE VIEW zendesk.{table_name} AS  SELECT * FROM delta.`{folder.path}`"
            spark.sql(table_create_statement)
