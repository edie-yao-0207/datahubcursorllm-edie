WITH customer_nps AS (
    SELECT
        date,
        raw_score,
        nps_score,
        sam_number
    FROM dataprep.customer_nps
)

SELECT
    n.date,
    n.sam_number,
    AVG(raw_score) AS avg_raw_score,
    AVG(nps_score) AS avg_nps_score
FROM customer_nps n
WHERE 
    n.date IS NOT NULL AND
    n.sam_number IS NOT NULL AND
    date >= ${start_date} AND
    date < ${end_date}
GROUP BY
    n.date,
    n.sam_number
