select * from dataprep_safety.all_crash_events_since_2022
where hidden_to_customer = false
and trip_start_ms > 0
and ms_after_trip_end > 0
