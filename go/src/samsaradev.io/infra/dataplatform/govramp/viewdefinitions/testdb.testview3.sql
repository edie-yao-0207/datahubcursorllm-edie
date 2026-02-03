SELECT device, COUNT(*) FROM {{SOME_TABLE}}
WHERE column = 'a'
GROUP BY device
UNION All
SELECT device, COUNT(*) FROM {{another_table}}
GROUP BY device