# Databricks notebook source
import json

"""
print_spoof_config takes in a locations trace spark data frame and prints out a json gps spoof.

trace_sdf should have the following columns, directly taken from the kinesisstats.location table:
* time
* lat
* lng

delay_factor is a multiplicative factor to lengthen/shorten the time between consecutive points in the spoof.
"""


def print_spoof_config(trace_sdf, delay_factor=0.5):
    trace_res = list(map(lambda row: row.asDict(), trace_sdf.collect()))

    spoof_points = []
    for idx, row in enumerate(trace_res):
        delay_ms = 0
        if idx > 0:
            # Compute delay
            delay_ms = delay_factor * (
                trace_res[idx]["time"] - trace_res[idx - 1]["time"]
            )

        point = {
            "delay_ms": delay_ms,
            "latitude": int(row["lat"] * 10**9),
            "longitude": int(row["lng"] * 10**9),
        }
        spoof_points.append(point)

    result = json.dumps({"gps_config": {"gps_spoof_entries": spoof_points}})
    print(result)
