CREATE OR REPLACE TEMP VIEW devices_free_trials
USING bigquery
OPTIONS (
  table  'netsuite_devices.devices_free_trials'
);
  
CREATE TABLE IF NOT EXISTS dataprep.devices_free_trials USING DELTA AS
(
  SELECT *
  FROM devices_free_trials
);

INSERT OVERWRITE TABLE dataprep.devices_free_trials
(
  SELECT *
  FROM devices_free_trials
);
