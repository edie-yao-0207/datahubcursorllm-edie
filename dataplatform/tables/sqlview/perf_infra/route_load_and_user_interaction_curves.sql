SELECT
  ms / 1000 as seconds,
  perf_infra.rlc(CAST(ms AS DOUBLE) / 1000) as rlc_score,
  perf_infra.uic(CAST(ms AS DOUBLE) / 1000) as uic_score
FROM
  (
    SELECT
      explode(sequence(0, 20000, 100)) AS ms
  )
