# Databricks notebook source
# MAGIC %run /backend/backend/bigquery/bigquery_pipeline

# COMMAND ----------

# IOS Specific Crashes
bigquery_pipeline(
    "d1-apps.firebase_crashlytics.com_samsara_Driver_IOS_REALTIME",  # Big Query Table Name
    "event_timestamp",  # Partition by date of event_timestamp
    "firebase_crashlytics",  # database for table
    "ios_driver_app",  # name of table
    days=14,
)

# Android Specific Crashes
bigquery_pipeline(
    "d1-apps.firebase_crashlytics.com_samsara_driver_ANDROID_REALTIME",  # Big Query Table Name
    "event_timestamp",  # Partition by date of event_timestamp
    "firebase_crashlytics",  # database for table
    "android_driver_app",  # name of table
    days=14,
)
