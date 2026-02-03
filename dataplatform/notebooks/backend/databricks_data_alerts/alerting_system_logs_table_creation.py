# MAGIC %run ./alerting_system_header

# COMMAND ----------

# IMPORTANT NOTE TO ENGINEER :
# If you change anything, please make run alerting_system_test_cases and ensure that all the tests to pass
# You must run the file on a dev cluster (not production cluster)

# This notebook is responsible for creating the alert_logs table in databricks_alerts

# Creating the table to which the alert log information is written
# Anytime an alert is run, the following is written to this table
create_logs_table_query = """
create table if not exists {log_db} (
  alert_name string,
  query string,
  emails array<string>,
  alert_timestamp timestamp, 
  alert_execution_time float, 
  row_count int,
  csv_url string,
  error_message string,
  slack_error_message string
 ) using delta
""".format(
    log_db=ALERTS_LOG
)
spark.sql(create_logs_table_query)
