SELECT *
FROM dataplatform_dev.sample_query_table
WHERE name = {{ text_param }}
AND cost > {{ numeric_param }}
AND datetime > '{{ datetime_param }}'
AND datetime between '{{ daterange_param.start }}' and '{{ daterange_param.end }}'
