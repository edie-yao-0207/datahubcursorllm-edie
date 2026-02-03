# MAGIC %sql
# MAGIC -- Computes an ECU speed correction factor per EBPMS install, given as average GPS speed / ECU speed
# MAGIC -- GPS speed is taken from our gateway reported location stat, while ECU speed is calculated from vehicle sensors and may be calibrated differently per device
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS firmware.ag53_ebs_speed_corr_factors_millik_ts (
# MAGIC   org_id BIGINT,
# MAGIC   object_id BIGINT,
# MAGIC   min_speed_ratio DECIMAL(38,9),
# MAGIC   max_speed_ratio DECIMAL(38,9),
# MAGIC   median_speed_ratio DOUBLE,
# MAGIC   num_samples BIGINT,
# MAGIC   calculation_time TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC INSERT INTO firmware.ag53_ebs_speed_corr_factors_millik_ts (
# MAGIC with devices as (select distinct object_id from firmware.ebs_installs_all),
# MAGIC
# MAGIC ecu_speed as (select org_id, object_id, date, time ,value.int_value * 1.852/1000 as speed_kph from kinesisstats.osdenginemilliknots where (value.int_value * 1.852 /1000 > 60) and object_id in (select object_id from devices) and date >= current_date() - 5),
# MAGIC
# MAGIC stats as (select ecu.*, gps.value.proto_value.nordic_gps_debug[0].gps_fix_info.speed_mm_per_s * 0.0036 gps_speed, ((gps.value.proto_value.nordic_gps_debug[0].gps_fix_info.speed_mm_per_s * 0.0036 )/ecu.speed_kph) as speed_gain ,
# MAGIC lead(ecu.speed_kph,1) over(partition by ecu.object_id order by ecu.time) as post1,
# MAGIC lag(ecu.speed_kph,1) over(partition by ecu.object_id order by ecu.time) as prev1,
# MAGIC lag(ecu.speed_kph,2) over(partition by ecu.object_id order by ecu.time) as prev2,
# MAGIC lag(ecu.speed_kph,3) over(partition by ecu.object_id order by ecu.time) as prev3
# MAGIC from ecu_speed ecu inner join kinesisstats.osdnordicgpsdebug gps on ecu.org_id = gps.org_id and ecu.object_id = gps.object_id and ecu.date = gps.date and gps.value.proto_value.nordic_gps_debug[0].gps_fix_info.hdop_thousandths < 1500 and  gps.time between ecu.time - 2500 and ecu.time+2500 and gps.date >= current_date() - 5),
# MAGIC
# MAGIC filtered as (select * from stats where greatest(prev1,speed_kph, prev2, prev3, post1) - least(prev1,speed_kph, prev2, prev3, post1) < 3)
# MAGIC
# MAGIC
# MAGIC select org_id, object_id, min(speed_gain) as min_speed_ratio, max(speed_gain) as max_speed_ratio, percentile(speed_gain, 0.5) as median_speed_ratio,
# MAGIC count(*) as num_samples, current_timestamp() as calculation_time from filtered where speed_gain between .70 and 1.30 group by org_id, object_id having num_samples > 100);
# MAGIC
