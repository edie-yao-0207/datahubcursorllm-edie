on_duration_ms <= 3600000 AND aux_during_idle_ms <= idle_duration_ms AND idle_duration_ms <= on_duration_ms
AND (engine_type IN (0,3,4) AND energy_consumed_kwh = 0) AND (engine_type = 1 AND fuel_consumed_ml = 0) -- We don't expect ICE/Hybrid/Hydrogen to consume energy, OR for EVs to consume fuel, PHEVs can consume either
