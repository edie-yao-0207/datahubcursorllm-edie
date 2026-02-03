# Databricks notebook source
# MAGIC %run /backend/backend/databricks_data_alerts/slack_utility

# COMMAND ----------

import http.client
import json
import pandas as pd
import numpy as np

# COMMAND ----------

df = spark.sql(
    """with dat as 
(select h.object_id, h.org_id, h.time, d.product_id, ceil((h.time/3600000)) as ceil_time
from kinesisstats.osdhubserverdeviceheartbeat as h
join clouddb.devices as d
on d.id = h.object_id
join clouddb.organizations as o
on o.id = h.org_id
where o.internal_type <> 1 
and d.product_id in (124, 125) 
and h.date >= current_date() - 1)

select cast(from_unixtime(x.ceil_time*3600) as timestamp) as time, x.product_id, y.estimate, count(distinct(object_id)) as no_devices
from dat as x
left join (select * 
            from hardware_analytics.estimated_heartbeats
            where product_id in (124, 125)
          ) as y
on x.ceil_time = y.ceil_time
and x.product_id = y.product_id
group by x.ceil_time, x.product_id, y.estimate
order by x.ceil_time
"""
).toPandas()

# COMMAND ----------

ag51_estimate = df[df["product_id"] == 124].estimate.to_numpy()[-9:-1]
ag51_actual = df[df["product_id"] == 124].no_devices.to_numpy()[-9:-1]
ag52_estimate = df[df["product_id"] == 125].estimate.to_numpy()[-9:-1]
ag52_actual = df[df["product_id"] == 125].no_devices.to_numpy()[-9:-1]

# COMMAND ----------

err = ag51_estimate - ag51_actual
err_perc = (err / ag51_estimate) * 100
outlies_ag51 = []
outlies_index_ag51 = []
for i, val in enumerate(err_perc):
    if val > 15:
        outlies_ag51.append(ag51_actual[i])
        outlies_index_ag51.append(i)


err = ag52_estimate - ag52_actual
err_perc = (err / ag52_estimate) * 100
outlies_ag52 = []
outlies_index_ag52 = []
for i, val in enumerate(err_perc):
    if val > 15:
        outlies_ag52.append(ag52_actual[i])
        outlies_index_ag52.append(i)


# COMMAND ----------

print(outlies_ag51, outlies_index_ag51)
print(outlies_ag52, outlies_index_ag52)

# COMMAND ----------

if len(outlies_ag51) > 1 or len(outlies_ag52) > 1:
    conn = http.client.HTTPSConnection("api.pagerduty.com")

    payload = """{
            \"incident\": {
                    \"type\": \"incident\",
                    \"title\": \"AG5X heartbeats less than estimated\",
                    \"service\": {
                                \"id\": \"PH273DQ\",
                                \"type\": \"service_reference\"
                                },
                    \"priority\": null,
                    \"urgency\": \"high\",
                    \"body\": {
                            \"type\": \"incident_body\",
                            \"details\": \"Total gateways with heartbeats was less than estimated by >15% in the last 4 hrs. Refer to KPI dashboard for more information \"
                            },
                    \"escalation_policy\": {
                            \"id\": \"P203YDC\",
                            \"type\": \"escalation_policy_reference\"
                        }
                    }
                }"""

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.pagerduty+json;version=2",
        "From": "wael.barakat@samsara.com",
        "Authorization": f"Token token={dbutils.secrets.get(scope = 'meenu_creds', key = 'pager_duty_api')}",
    }

    conn.request("POST", "/incidents", payload, headers)

    res = conn.getresponse()
    data = res.read()
    send_slack_message_to_channels(data.decode("utf-8"), ["alerts-hw-analytics"])
    incident_created = json.loads(data.decode("utf-8"))

    send_slack_message_to_channels(
        incident_created["incident"]["html_url"], ["alerts-hw-analytics"]
    )


# COMMAND ----------

import matplotlib.pyplot as plt

plt.plot(ag51_estimate)
plt.plot(ag51_actual)
plt.plot(ag52_estimate)
plt.plot(ag52_actual)
