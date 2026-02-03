# Databricks notebook source
from typing import List, Tuple
from datetime import datetime
import requests
import json
import time

# COMMAND ----------

"""
    Helper to get the total count first before we batch-fetch the whole dataset
    
    e.g. 
    Input: https://gis.iowadot.gov/agshost/rest/services/RAMS/Road_Network/FeatureServer/0/query?where=1%3D1&outFields=*&returnCountOnly=true&outSR=4326&f=json
    
    Output: 363274
"""


def get_total_count_from_arcgis_dataset_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for any HTTP error
        data = response.json()
        return data["count"]
    except Exception as e:
        raise Exception(f"Error {e} fetching dataset count from URL: {url}")


# COMMAND ----------

"""
    Helper to fetch data in batches from the ArcGIS REST API endpoint, retry with exponential backoff

    e.g. https://gis.iowadot.gov/agshost/rest/services/RAMS/Road_Network/FeatureServer/0/query?where=1%3D1&outFields=ID,ROUTEID,FROMMEASURE,TOMEASURE,SPEED_LIMIT,EDITDATE,RUN_DATE,GLOBALID&outSR=4326&f=json&resultOffset=0&resultRecordCount=2000
"""


def retryable_fetch(base_url, offset, count, max_attempts=5):
    attempts = 0
    res = None
    while attempts < max_attempts:
        try:
            url = f"{base_url}&resultOffset={offset}&resultRecordCount={count}"

            print(
                f"Start fetching from {offset+1}th to {offset+count}th, with url={url}"
            )

            response = requests.get(url)
            attempts += 1

            response.raise_for_status()  # raise an exception for any HTTP error, and trigger a retry
            data = response.json()

            if not data or "features" not in data or len(data["features"]) == 0:
                """
                2 cases that we should retry:
                - No data returned
                - Rate limit reached, e.g. NA DOT returns an empty object instead of an error
                """
                raise Exception(
                    f"No data returned while fetching from {offset+1}th to {offset+count}th"
                )

            print(f"Suceeded to fetch from {offset+1}th to {offset+count}th")

            res = data["features"]
            break
        except Exception as e:
            backoff_time = 5**attempts

            print(
                f"Error fetching from {offset+1}th to {offset+count}th: {e}. Retrying in {backoff_time} seconds..."
            )

            time.sleep(backoff_time)  # unit is in second

    if not res:
        raise Exception(
            f"Failed to fetch from {offset+1}th to {offset+count}th after {max_attempts} attempts"
        )
    return res


def fetch_dataset_in_batches(
    base_url, dataset_total_count, max_retries=5, ms_to_avoid_rate_limit=1000
):
    RESULT_RECORD_COUNT = 2000
    current_offset = 0

    all_rows = []

    while len(all_rows) < dataset_total_count:
        rows = retryable_fetch(
            base_url, current_offset, RESULT_RECORD_COUNT, max_retries
        )
        all_rows += rows
        current_offset += RESULT_RECORD_COUNT
        time.sleep(ms_to_avoid_rate_limit / 1000)

    print(f"Downloads Completed, total {len(all_rows)} rows")
    return all_rows
