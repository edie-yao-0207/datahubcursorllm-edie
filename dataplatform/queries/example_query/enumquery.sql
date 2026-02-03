SELECT *
FROM dataplatform_dev.sample_query_table
WHERE name in ({{ enum_param }})
