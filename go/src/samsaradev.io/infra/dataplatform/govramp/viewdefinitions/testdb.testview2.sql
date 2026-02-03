SELECT device, COUNT(*) FROM anotherdb.underlying_table
WHERE column = 'a'
GROUP BY device
UNION All
SELECT device, COUNT(*) FROM anotherdb.underlying_table
WHERE column = 'b'
GROUP BY device
UNION All
SELECT device, COUNT(*) FROM yetanotherdb.underlying_table2
GROUP BY device