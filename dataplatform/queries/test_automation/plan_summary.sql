SELECT test_plan, product, passed, datetime, duration_ms / 1000 / 60 / 60 AS duration_hr, app_name,
    CASE 
        WHEN app_name != "" THEN app_name
        WHEN product LIKE 'CM%' THEN 'cm'
        WHEN product LIKE 'AG%' THEN 'marathon'
        WHEN product LIKE 'VG%' THEN 'vg'
        WHEN product = 'AHD1' THEN 'octo'
        WHEN product = 'AT11' THEN 'hansel'
        ELSE LOWER(product)
        END AS APP
FROM owltomation_results.plan_results
WHERE datetime BETWEEN '{{ daterange_param.start }}' AND '{{ daterange_param.end }}'
AND product NOT IN ('template')
ORDER BY product
