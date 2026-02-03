-- This view is dependent on manually creating a table in netsuite_data.netsuite_child_invoice_last_successful_run
-- that keeps track of the timestamp of each run of the notebook.
SELECT MAX(last_run) as most_recent FROM netsuite_data.netsuite_child_invoice_last_successful_run
