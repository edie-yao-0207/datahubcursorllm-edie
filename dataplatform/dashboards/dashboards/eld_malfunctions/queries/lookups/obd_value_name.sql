SELECT DISTINCT name
FROM definitions.obd_values
WHERE name IS NOT NULL
ORDER BY name

