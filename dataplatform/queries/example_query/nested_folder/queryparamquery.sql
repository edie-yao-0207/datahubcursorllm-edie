SELECT *
FROM dataplatform_dev.sample_query_table
WHERE name in ({{ query_param_name }})
OR name in ({{ query_param_uuid }})
