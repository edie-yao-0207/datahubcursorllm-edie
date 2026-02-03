-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Overview
-- MAGIC
-- MAGIC This notebook uploads a CSV file to S3 to be used as source for gateway-level rollout stage rebalancing for the default program (GA program).
-- MAGIC It also writes the output of the rebalancing to a Delta table in the firmware_dev database, to be used as input for the visualization notebook.
-- MAGIC
-- MAGIC **DO NOT** use this notebook for:
-- MAGIC * Custom-program rollout stage enrollment.
-- MAGIC * Org-level rollout stage balancing.
-- MAGIC
-- MAGIC The rebalance is performed according to the following rules:
-- MAGIC * Internal orgs are excluded.
-- MAGIC * All devices are assigned to a rollout stage using the gateway-based rollout stage framework. No existing device is left unassigned.
-- MAGIC * Escalated customers' devices (yellow/red sentiment) are assigned to the 100s stages, starting from stage 500 downwards (400, 300, ...) based on number of devices. Escalated devices must never be in the 1000s stages.
-- MAGIC * Each stage is filled up with:
-- MAGIC   * 80% of high-activity devices: active for 21 days or more in the last 28, as per `datamodel_core.lifetime_device_activity` definitions.
-- MAGIC   * 20% of low-activity devices: active for 2 days or more in the last 28.
-- MAGIC * Inactive devices (that don't fall into the above two buckets) are all assigned to a given stage (configurable).
-- MAGIC * If a stage is over-filled, devices will be unassigned from that stage and re-assigned as needed to other stages.
-- MAGIC
-- MAGIC ### How to use
-- MAGIC
-- MAGIC * [How to update firmware rollout stages](https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5041222/How+to+Update+Firmware+Rollout+Stages)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from collections import OrderedDict
-- MAGIC
-- MAGIC products_df = spark.sql("select name from definitions.products order by 1")
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.dropdown("product_name", "VG54-NA", [a.name for a in products_df.collect() if a.name is not None and not a.name.startswith("Mobile")], "Product")
-- MAGIC dbutils.widgets.dropdown("reassign_all_devices", "False", ["True", "False"], "Reassign all gateways")
-- MAGIC
-- MAGIC VG_CONFIG = {
-- MAGIC   "target_gateway_volume_by_stage": OrderedDict({
-- MAGIC     # 1000: VDP-reserved, balanced separately to have a proper split of MMY.
-- MAGIC     2000: 2,
-- MAGIC     3000: 2,
-- MAGIC     4000: 7.5,
-- MAGIC     5000: 7.5,
-- MAGIC     6000: 10,
-- MAGIC     7000: 10,
-- MAGIC     8000: 10,
-- MAGIC     # 9000: unused.
-- MAGIC     # Manual Stage: used for early rollout to internal orgs.
-- MAGIC     # Field Test Stage: used for gateways we can't upgrade.
-- MAGIC
-- MAGIC     100:  10,
-- MAGIC     200:  10,
-- MAGIC     300:  10,
-- MAGIC     400:  10,
-- MAGIC     500:  10,
-- MAGIC     # 600: it's our `inactive_devices_stage`.
-- MAGIC     # 700: keep it empty for now, we might use it later.
-- MAGIC   }),
-- MAGIC   "inactive_devices_stage": 600, # Stage that inactive devices will be assigned to.
-- MAGIC   "reassign_all_devices_stage_exclusion": [1000], # exclusion list for the "reassign_all_devices" widget config
-- MAGIC }
-- MAGIC
-- MAGIC AG_CONFIG = {
-- MAGIC   "target_gateway_volume_by_stage": OrderedDict({
-- MAGIC     # 1000: reserved for internal org units
-- MAGIC     # 2000: reserved for beta units
-- MAGIC     3000: 1,
-- MAGIC     4000: 2,
-- MAGIC     5000: 5,
-- MAGIC     6000: 7.5,
-- MAGIC     7000: 10,
-- MAGIC     8000: 15,
-- MAGIC     9000: 20,
-- MAGIC     # Manual Stage: note used
-- MAGIC     # Field Test Stage: not used
-- MAGIC
-- MAGIC     100:  10,
-- MAGIC     200:  10,
-- MAGIC     300:  10,
-- MAGIC     400:  10,
-- MAGIC     500:  10,
-- MAGIC     # 600: it's our `inactive_devices_stage`.
-- MAGIC     # 700: keep it empty for now, we might use it later.
-- MAGIC   }),
-- MAGIC   "inactive_devices_stage": 600, # Stage that inactive devices will be assigned to.
-- MAGIC   "reassign_all_devices_stage_exclusion": [1000, 2000], # exclusion list for the "reassign_all_devices" widget config
-- MAGIC }
-- MAGIC
-- MAGIC CM_CONFIG = {
-- MAGIC   "target_gateway_volume_by_stage": OrderedDict({
-- MAGIC     # All 1000s stages add up to 100% of active devices to account
-- MAGIC     # for the case when all devices are active and not escalated.
-- MAGIC     1000: 0.5,
-- MAGIC     2000: 3.5,
-- MAGIC     3000: 6,
-- MAGIC     4000: 10,
-- MAGIC     5000: 30,
-- MAGIC     6000: 50,
-- MAGIC     # 7000: unused.
-- MAGIC     # 8000: unused.
-- MAGIC     # 9000: unused.
-- MAGIC     # Manual Stage: used for early rollout to internal orgs.
-- MAGIC     # Field Test Stage: used for gateways we can't upgrade.
-- MAGIC
-- MAGIC     # All escalated stages add up to 100% of active devices to account
-- MAGIC     # for the extreme case when the majority of devices are active and
-- MAGIC     # escalated and we don't want them overflowing to 1000s stages.
-- MAGIC     100:  20, # Escalated devices overflowed from stage 200.
-- MAGIC     200:  20, # Escalated devices overflowed from stage 300.
-- MAGIC     300:  20, # Escalated devices overflowed from stage 400.
-- MAGIC     400:  20, # Escalated devices overflowed from stage 500.
-- MAGIC     500:  20, # Default stage for escalated devices.
-- MAGIC     # 600: it's our `inactive_devices_stage`.
-- MAGIC     # 700: unused.
-- MAGIC   }),
-- MAGIC   "inactive_devices_stage": 600, # Stage that inactive devices will be assigned to.
-- MAGIC   "reassign_all_devices_stage_exclusion": [], # exclusion list for the "reassign_all_devices" widget config
-- MAGIC }
-- MAGIC
-- MAGIC BLE_PERIPH_CONFIG = {
-- MAGIC   "target_gateway_volume_by_stage": OrderedDict({
-- MAGIC     # 1000: reserved for internal org units
-- MAGIC     # 2000: reserved for beta units
-- MAGIC     3000: 1,
-- MAGIC     4000: 2,
-- MAGIC     5000: 5,
-- MAGIC     6000: 7.5,
-- MAGIC     7000: 10,
-- MAGIC     8000: 15,
-- MAGIC     9000: 20,
-- MAGIC     # Manual Stage: note used
-- MAGIC     # Field Test Stage: not used
-- MAGIC
-- MAGIC     100:  10,
-- MAGIC     200:  10,
-- MAGIC     300:  10,
-- MAGIC     400:  10,
-- MAGIC     500:  10,
-- MAGIC     # 600: it's our `inactive_devices_stage`.
-- MAGIC     # 700: keep it empty for now, we might use it later.
-- MAGIC   }),
-- MAGIC   "inactive_devices_stage": 600, # Stage that inactive devices will be assigned to.
-- MAGIC   "reassign_all_devices_stage_exclusion": [1000, 2000], # exclusion list for the "reassign_all_devices" widget config
-- MAGIC }
-- MAGIC
-- MAGIC # Stages in `target_gateway_volume_by_stage` must be listed in the same
-- MAGIC # order as they will be rolled out to.
-- MAGIC # Stages not defined in the target_gateway_volume_by_stage` config field will not be rebalanced, see comments.
-- MAGIC # The target % is for active devices (see activity definition above).
-- MAGIC # If the `reassign_all_devices` widget is set to "True", all existing assignments will be discarded and balancing will be
-- MAGIC # performed from scratch (except for stages listed in `reassign_all_devices_stage_exclusion`).
-- MAGIC REBALANCE_CONFIG = {
-- MAGIC   "VG34": VG_CONFIG,
-- MAGIC   "VG54-NA": VG_CONFIG,
-- MAGIC   "VG55-NA": VG_CONFIG,
-- MAGIC   "VG34-EU": VG_CONFIG,
-- MAGIC   "VG54-EU": VG_CONFIG,
-- MAGIC   "AG51": AG_CONFIG,
-- MAGIC   "AG51-EU": AG_CONFIG,
-- MAGIC   "AG52": AG_CONFIG,
-- MAGIC   "AG52-EU": AG_CONFIG,
-- MAGIC   "AG53": AG_CONFIG,
-- MAGIC   "AG53-EU": AG_CONFIG,
-- MAGIC   "CM31": CM_CONFIG,
-- MAGIC   "CM32": CM_CONFIG,
-- MAGIC   "CM33": CM_CONFIG,
-- MAGIC   "CM34": CM_CONFIG,
-- MAGIC   "AHD1": CM_CONFIG,
-- MAGIC   "AHD4": CM_CONFIG,
-- MAGIC   "AT11": BLE_PERIPH_CONFIG,
-- MAGIC   "AT11X": BLE_PERIPH_CONFIG,
-- MAGIC   "LM11": BLE_PERIPH_CONFIG,
-- MAGIC   "CW21": BLE_PERIPH_CONFIG,
-- MAGIC   "EM21": BLE_PERIPH_CONFIG,
-- MAGIC   "EM22": BLE_PERIPH_CONFIG,
-- MAGIC   "EM23": BLE_PERIPH_CONFIG,
-- MAGIC   "EM31": BLE_PERIPH_CONFIG,
-- MAGIC   "ACC-DM11": BLE_PERIPH_CONFIG,
-- MAGIC   "ACC-CRGO": BLE_PERIPH_CONFIG,
-- MAGIC }
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Create a table of all devices with their rollout stages and customer sentiment

-- MAGIC %python
-- MAGIC product_name = dbutils.widgets.get("product_name")
-- MAGIC LEGACY_MONITORS = {'EM21','EM22','EM23','EM31','ACC-DM11','ACC-CRGO'}
-- MAGIC # Legacy monitors do not have activity data in the lifetime_device_activity table, therefore we need to use the widgets table instead.
-- MAGIC if product_name in LEGACY_MONITORS:
-- MAGIC     spark.sql(f"""
-- MAGIC       create or replace temporary view allActiveDevices as
-- MAGIC       select
-- MAGIC         widget.id as device_id,
-- MAGIC         m.org_id,
-- MAGIC         m.product_id,
-- MAGIC         m.id as gateway_id,
-- MAGIC         null as activity -- Legacy monitors do not have activity data like other gateways, hence activity is set to null.
-- MAGIC       from productsdb.widgets widget
-- MAGIC       left join productsdb.monitors m
-- MAGIC         on widget.id = m.widget_id
-- MAGIC       left join datamodel_core.dim_organizations o
-- MAGIC         on m.org_id = o.org_id
-- MAGIC       where
-- MAGIC         o.date = current_date()-2
-- MAGIC         and m.product_id = (
-- MAGIC           select product_id from definitions.products where name = '{product_name}'
-- MAGIC         )
-- MAGIC         and o.internal_type_name = 'Customer Org'
-- MAGIC         and m.id is not null
-- MAGIC     """)
-- MAGIC else:
-- MAGIC     spark.sql(f"""
-- MAGIC       create or replace temporary view allActiveDevices as
-- MAGIC       select
-- MAGIC         da.device_id,
-- MAGIC         da.org_id,
-- MAGIC         g.product_id,
-- MAGIC         g.id as gateway_id,
-- MAGIC         case
-- MAGIC           when da.l28 >= 21 then 'high'
-- MAGIC           when da.l28 >= 2 then 'low'
-- MAGIC           else 'inactive'
-- MAGIC         end as activity
-- MAGIC       from datamodel_core.lifetime_device_activity da
-- MAGIC       left join productsdb.gateways g
-- MAGIC         on da.device_id = g.device_id
-- MAGIC       left join datamodel_core.dim_organizations o
-- MAGIC         on da.org_id = o.org_id
-- MAGIC       where
-- MAGIC         da.date = current_date()-2
-- MAGIC         and o.date = current_date()-2
-- MAGIC         and g.product_id = (
-- MAGIC           select product_id from definitions.products where name = '{product_name}'
-- MAGIC         )
-- MAGIC         and o.internal_type_name = 'Customer Org'
-- MAGIC         and g.id is not null -- exclude devices without gateways
-- MAGIC     """)

create or replace temporary view allActiveDevicesWithStages as (
  select
    d.org_id,
    d.product_id,
    d.device_id,
    d.gateway_id,
    grs.rollout_stage_id,
    d.activity
  from allActiveDevices as d
  left join firmwaredb.gateway_rollout_stages as grs
    on d.gateway_id = grs.gateway_id
    and d.org_id = grs.org_id
    and d.product_id = grs.product_id
);

create or replace temporary view orgCsmRatings as (
  select
    org_id,
    max(struct(date, csm_rating)).csm_rating as csm_rating
  from customer360.customer_health_tracker
  group by org_id
);

create or replace temporary view allActiveDevicesWithStagesAndSentiment as (
  select
    d.org_id,
    d.product_id,
    d.device_id,
    d.gateway_id,
    d.rollout_stage_id,
    c.csm_rating,
    d.activity
  from allActiveDevicesWithStages as d
  left join orgCsmRatings as c
    on  d.org_id = c.org_id
);

create or replace temporary view currentDeviceCountsByStage as (
  select
    count(*) as cnt,
    rollout_stage_id,
    product_id
  from allActiveDevicesWithStages
  group by rollout_stage_id, product_id
  order by rollout_stage_id
);


-- COMMAND ----------

select count(distinct device_id) as active_devices from allActiveDevices

-- COMMAND ----------

select activity, count(distinct device_id) as active_devices from allActiveDevices group by 1 order by 1

-- COMMAND ----------

select product_id, rollout_stage_id, cnt from currentDeviceCountsByStage order by 1,2

-- COMMAND ----------

-- DBTITLE 1,Utils
-- MAGIC %python
-- MAGIC
-- MAGIC from collections import defaultdict
-- MAGIC
-- MAGIC class DeviceWithStage:
-- MAGIC   """
-- MAGIC   Represents a device and its assigned rollout stage, along with a few other attributes.
-- MAGIC   """
-- MAGIC   def __init__(self, org_id, product_id, device_id, gateway_id, rollout_stage_id, csm_rating, activity):
-- MAGIC     self._org_id = org_id
-- MAGIC     self._product_id = product_id
-- MAGIC     self._device_id = device_id
-- MAGIC     self._gateway_id = gateway_id
-- MAGIC     self._rollout_stage_id = rollout_stage_id
-- MAGIC     self._csm_rating = csm_rating.lower() if csm_rating is not None else "green"
-- MAGIC     self._activity = activity
-- MAGIC
-- MAGIC   @staticmethod
-- MAGIC   def fromDF(dataframe):
-- MAGIC     return DeviceWithStage(
-- MAGIC       dataframe["org_id"], dataframe["product_id"], dataframe["device_id"], dataframe["gateway_id"], dataframe["rollout_stage_id"], dataframe["csm_rating"], dataframe["activity"]
-- MAGIC     )
-- MAGIC
-- MAGIC   def __str__(self):
-- MAGIC     return f"{self._org_id}, {self._product_id}, {self._device_id}, {self._gateway_id}, {self._rollout_stage_id}, {self._csm_rating}, {self._activity}"
-- MAGIC
-- MAGIC   def get_rollout_stage_id(self):
-- MAGIC     return self._rollout_stage_id
-- MAGIC
-- MAGIC   def get_org_id(self):
-- MAGIC     return self._org_id
-- MAGIC
-- MAGIC   def move_to_stage(self, new_stage):
-- MAGIC     self._rollout_stage_id = new_stage
-- MAGIC
-- MAGIC   def is_assigned(self) -> bool:
-- MAGIC     return self._rollout_stage_id is not None
-- MAGIC
-- MAGIC   def is_green_sentiment(self) -> bool:
-- MAGIC     return self._csm_rating == "green"
-- MAGIC
-- MAGIC   def get_activity(self) -> str | None:
-- MAGIC     # Since legacy monitors do not have activity data, we return None.
-- MAGIC     if self._activity is None:
-- MAGIC       return None
-- MAGIC     return self._activity
-- MAGIC
-- MAGIC   def get_csm_rating(self) -> str:
-- MAGIC     return self._csm_rating
-- MAGIC
-- MAGIC def deviceCountsByStage(devices) -> dict:
-- MAGIC   """
-- MAGIC   Returns dictionary of rollout stage to number of gateways assigned to that stage,
-- MAGIC   from a list of DeviceWithStage.
-- MAGIC   """
-- MAGIC   count_by_stage = defaultdict(lambda: 0)
-- MAGIC   for d in devices:
-- MAGIC     rollout_stage_id = d.get_rollout_stage_id()
-- MAGIC     count_by_stage[rollout_stage_id] += 1
-- MAGIC   return count_by_stage
-- MAGIC
-- MAGIC
-- MAGIC def printCountsWithPercent(currentCountsDict, allDeviceCount, activeDeviceCount):
-- MAGIC   for stage in currentCountsDict:
-- MAGIC     if stage == 600:
-- MAGIC       print(f"Stage {stage}: {currentCountsDict[stage]} ({currentCountsDict[stage]/allDeviceCount * 100:.2f}% of total)")
-- MAGIC     else:
-- MAGIC       print(f"Stage {stage}: {currentCountsDict[stage]} ({currentCountsDict[stage]/allDeviceCount * 100:.2f}% of total, {currentCountsDict[stage]/activeDeviceCount * 100:.2f}% of active)")
-- MAGIC
-- MAGIC
-- MAGIC def devicesWithSentiment(devices, sentiment):
-- MAGIC   """
-- MAGIC   Returns subset of devices with the given sentiment.
-- MAGIC   """
-- MAGIC   return [device for device in devices if device.get_csm_rating() == sentiment.lower()]
-- MAGIC
-- MAGIC
-- MAGIC def move_device_to_stage(device, stage_counts, new_stage) -> None:
-- MAGIC   """
-- MAGIC   Moves device to new_stage and increments/decrements counts for previous/new stage as needed.
-- MAGIC   """
-- MAGIC   stage_counts[new_stage] = stage_counts.get(new_stage, 0) + 1
-- MAGIC   stage_counts[device.get_rollout_stage_id()] -= 1
-- MAGIC   device.move_to_stage(new_stage)
-- MAGIC
-- MAGIC
-- MAGIC def unassign_devices(devices, stage_counts) -> None:
-- MAGIC   """
-- MAGIC   Moves devices to the None stage to unassign them.
-- MAGIC   """
-- MAGIC   for device in devices:
-- MAGIC     move_device_to_stage(device, stage_counts, None)
-- MAGIC
-- MAGIC
-- MAGIC def find_next_unfilled_stage(stage_start, stage_iter, stage_counts, target_count_by_stage):
-- MAGIC   """
-- MAGIC   Returns the first stage in stage_iter (starting from stage_start) that is not filled,
-- MAGIC   based on current counts (stage_counts) and target count (target_count_by_stage).
-- MAGIC   Returns a tuple with the stage and True/False depending on whether there's still an unfilled stage.
-- MAGIC   """
-- MAGIC   current_stage = stage_start
-- MAGIC   while stage_counts.get(current_stage, 0) >= target_count_by_stage[current_stage]:
-- MAGIC     try:
-- MAGIC       current_stage = next(stage_iter)
-- MAGIC     except StopIteration:
-- MAGIC       return None
-- MAGIC   return current_stage
-- MAGIC
-- MAGIC
-- MAGIC def stagesToFitDevices(num_devices, target_count_by_stage, reverse_order=False):
-- MAGIC   """
-- MAGIC   Returns a list of the stages that are needed to fit the number devices provided.
-- MAGIC   """
-- MAGIC   stages = []
-- MAGIC   consumed_stage_space = 0
-- MAGIC
-- MAGIC   if reverse_order:
-- MAGIC     iterator = reversed(target_count_by_stage.items())
-- MAGIC   else:
-- MAGIC     iterator = target_count_by_stage.items()
-- MAGIC
-- MAGIC   for stage, count in iterator:
-- MAGIC     if num_devices > consumed_stage_space:
-- MAGIC       stages.append(stage)
-- MAGIC       consumed_stage_space += count
-- MAGIC     else:
-- MAGIC       break
-- MAGIC
-- MAGIC   return stages
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Functions to move devices in/out of stages depending on available space in that stage
-- MAGIC %python
-- MAGIC
-- MAGIC def moveUnassignedDevicesIntoStages(devices, stage_counts, target_count_by_stage, activity_requirement:str):
-- MAGIC   """
-- MAGIC   Move devices into new stages until stage_counts match target_count_by_stage.
-- MAGIC   Devices that are already assigned are not moved.
-- MAGIC   """
-- MAGIC   # First entry in target_count_by_stage ordered dict.
-- MAGIC   stage_iter = iter(target_count_by_stage)
-- MAGIC   current_stage = next(stage_iter)
-- MAGIC
-- MAGIC   num_devices_moved = 0
-- MAGIC   for device in devices:
-- MAGIC     if device.is_assigned():
-- MAGIC       continue
-- MAGIC
-- MAGIC     if activity_requirement is not None and device.get_activity() != activity_requirement:
-- MAGIC       continue
-- MAGIC
-- MAGIC     current_stage = find_next_unfilled_stage(current_stage, stage_iter, stage_counts, target_count_by_stage)
-- MAGIC     if current_stage is None:
-- MAGIC       break
-- MAGIC
-- MAGIC     if stage_counts.get(current_stage, 0) < target_count_by_stage[current_stage]:
-- MAGIC       move_device_to_stage(device, stage_counts, current_stage)
-- MAGIC       num_devices_moved += 1
-- MAGIC     else:
-- MAGIC       print(f"Error! Should not happen: Skipping device: {device}")
-- MAGIC
-- MAGIC   return num_devices_moved
-- MAGIC
-- MAGIC
-- MAGIC def moveUnassignedDevicesIntoStagesByActivity(devices, stage_counts, target_count_by_stage, target_count_by_stage_activity, activity_requirement:str):
-- MAGIC   """
-- MAGIC   Move devices into new stages until stage_counts match target_count_by_stage or target_count_by_stage_activity.
-- MAGIC   Devices that are already assigned are not moved.
-- MAGIC   """
-- MAGIC   # First entry in target_count_by_stage ordered dict.
-- MAGIC   stage_iter = iter(target_count_by_stage)
-- MAGIC   current_stage = next(stage_iter)
-- MAGIC
-- MAGIC   # Count devices by stage for the activity requirement.
-- MAGIC   stage_activity_counts = {}
-- MAGIC   for device in devices:
-- MAGIC     if device.is_assigned() and device.get_activity() == activity_requirement:
-- MAGIC       stage_activity_counts[device.get_rollout_stage_id()] = stage_activity_counts.get(device.get_rollout_stage_id(), 0) + 1
-- MAGIC
-- MAGIC   num_devices_moved = 0
-- MAGIC   for device in devices:
-- MAGIC     if device.is_assigned():
-- MAGIC       continue
-- MAGIC
-- MAGIC     if device.get_activity() != activity_requirement:
-- MAGIC       continue
-- MAGIC
-- MAGIC     # Find the next stage that has space for the device taking into account
-- MAGIC     # the total target count and the target count for the activity requirement.
-- MAGIC     while stage_counts.get(current_stage, 0) >= target_count_by_stage[current_stage] or \
-- MAGIC       stage_activity_counts.get(current_stage, 0) >= target_count_by_stage_activity[current_stage]:
-- MAGIC       try:
-- MAGIC         current_stage = next(stage_iter)
-- MAGIC       except StopIteration:
-- MAGIC         current_stage = None
-- MAGIC         break
-- MAGIC
-- MAGIC     if current_stage is None:
-- MAGIC       break
-- MAGIC
-- MAGIC     # Move the device to the stage.
-- MAGIC     move_device_to_stage(device, stage_counts, current_stage)
-- MAGIC     # Update the activity count for the stage.
-- MAGIC     move_device_to_stage(device, stage_activity_counts, current_stage)
-- MAGIC     num_devices_moved += 1
-- MAGIC
-- MAGIC   return num_devices_moved
-- MAGIC
-- MAGIC
-- MAGIC def moveUnassignedDevicesIntoStagesByOrg(sorted_devices, stage_counts, target_count_by_stage, reverse_order=False) -> int:
-- MAGIC   """
-- MAGIC   Move sorted_devices into new stages until stage_counts match target_count_by_stage.
-- MAGIC   Makes sure that all devices belonging to an organization are enrolled in the same stage.
-- MAGIC   Devices that are already assigned are not moved.
-- MAGIC   Return number of devices moved.
-- MAGIC   """
-- MAGIC   # First entry in target_count_by_stage ordered dict.
-- MAGIC   stage_iter = iter(target_count_by_stage)
-- MAGIC   if reverse_order:
-- MAGIC     stage_iter = iter(reversed(target_count_by_stage))
-- MAGIC   current_stage = next(stage_iter)
-- MAGIC
-- MAGIC   last_inserted_org = 0
-- MAGIC   moved = 0
-- MAGIC   for device in sorted_devices:
-- MAGIC     if device.is_assigned():
-- MAGIC       continue
-- MAGIC
-- MAGIC     # Get the next stage key, but only set it when we detect an org change.
-- MAGIC     if device.get_org_id() != last_inserted_org:
-- MAGIC       current_stage = find_next_unfilled_stage(current_stage, stage_iter, stage_counts, target_count_by_stage)
-- MAGIC       if current_stage is None:
-- MAGIC         break
-- MAGIC
-- MAGIC     move_device_to_stage(device, stage_counts, current_stage)
-- MAGIC     last_inserted_org = device.get_org_id()
-- MAGIC     moved += 1
-- MAGIC
-- MAGIC   return moved
-- MAGIC
-- MAGIC
-- MAGIC def removeNumberOfDevicesFromStages(devices, num_to_remove, target_stage, stage_counts, target_count_by_stage) -> int:
-- MAGIC   """
-- MAGIC   Removes up to num_to_remove devices from the target_stage and returns the number that could not be removed.
-- MAGIC   If target_stage has more than num_to_remove devices, removes num_to_remove and returns 0.
-- MAGIC   If target_stage does not have num_to_remove devices, removes them all and returns the difference.
-- MAGIC   Returns number of devices actually removed.
-- MAGIC   """
-- MAGIC   remaining = num_to_remove
-- MAGIC
-- MAGIC   moved = 0
-- MAGIC   for device in devices:
-- MAGIC     if remaining <= 0:
-- MAGIC       return 0
-- MAGIC     if device.get_rollout_stage_id() != target_stage:
-- MAGIC       continue
-- MAGIC     if stage_counts[target_stage] <= 0:
-- MAGIC       break
-- MAGIC     move_device_to_stage(device, stage_counts, None)
-- MAGIC     remaining -= 1
-- MAGIC     moved += 1
-- MAGIC   print(f"Moved {moved} devices from stage {target_stage} to be unassigned.")
-- MAGIC   return remaining
-- MAGIC
-- MAGIC
-- MAGIC def removeDevicesFromOverfilledStages(devices, stage_counts, target_count_by_stage) -> int:
-- MAGIC   """
-- MAGIC   Removes devices from overfilled stages, but will not move Red/Yellow Sentiment devices.
-- MAGIC   Returns number of devices removed.
-- MAGIC   """
-- MAGIC   current_stage = next(iter(target_count_by_stage))
-- MAGIC
-- MAGIC   moved = 0
-- MAGIC   for device in devices:
-- MAGIC     current_stage = device.get_rollout_stage_id()
-- MAGIC     if current_stage is None:
-- MAGIC       continue # no point moving from None to None
-- MAGIC     if current_stage not in target_count_by_stage:
-- MAGIC       continue # device's current stage isn't one being targeted
-- MAGIC     if not device.is_green_sentiment():
-- MAGIC       continue # Function will not alter red/yellow devices
-- MAGIC
-- MAGIC     if stage_counts.get(current_stage, 0) > target_count_by_stage[current_stage]:
-- MAGIC       move_device_to_stage(device, stage_counts, None)
-- MAGIC       moved += 1
-- MAGIC
-- MAGIC   return moved
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Execute queries to grab raw data
-- MAGIC %python
-- MAGIC
-- MAGIC # Get all devices and the stages they belong to.
-- MAGIC active_devices_count = spark.sql("select count(*) from allActiveDevicesWithStagesAndSentiment where activity != 'inactive'").first()[0]
-- MAGIC all_devices_df = spark.sql("select * from allActiveDevicesWithStagesAndSentiment").collect()
-- MAGIC all_devices_count = len(all_devices_df)
-- MAGIC
-- MAGIC print("Total devices: " + str(all_devices_count))
-- MAGIC print("Total active devices: " + str(active_devices_count))

-- COMMAND ----------

-- DBTITLE 1,Perform the actual calculations to rebalance
-- MAGIC %python
-- MAGIC import random
-- MAGIC from collections import OrderedDict
-- MAGIC
-- MAGIC # Get config.
-- MAGIC product_name = dbutils.widgets.get("product_name")
-- MAGIC config = REBALANCE_CONFIG[product_name]
-- MAGIC
-- MAGIC # Convert dataframe rows to list, and randomize.
-- MAGIC devices = [DeviceWithStage.fromDF(device) for device in all_devices_df]
-- MAGIC random.shuffle(devices)
-- MAGIC
-- MAGIC # Get initial counts
-- MAGIC current_device_count_by_stage = deviceCountsByStage(devices)
-- MAGIC print(f"Devices by stage before rebalance: {current_device_count_by_stage}")
-- MAGIC
-- MAGIC # Check if the product is a legacy monitor that does not have activity data.
-- MAGIC # If it is, we will use the all_devices_count instead of the active_devices_count for target device counts.
-- MAGIC has_activity_logic = product_name not in LEGACY_MONITORS
-- MAGIC # Multiply percentage target (hard-coded, top of notebook) by active devices count to get target count in each stage.
-- MAGIC # Note that inactive devices are excluded from active_devices_count so that we don't populate stages with devices
-- MAGIC # that won't actually upgrade to the firmware when we rollout (because they are disconnected, don't go on trips, etc).
-- MAGIC target_device_counts_by_stage = OrderedDict()
-- MAGIC for stage, percent in config["target_gateway_volume_by_stage"].items():
-- MAGIC   if has_activity_logic:
-- MAGIC     target_device_counts_by_stage[stage] = int(active_devices_count * (percent * 0.01))
-- MAGIC   else:
-- MAGIC     target_device_counts_by_stage[stage] = int(all_devices_count * (percent * 0.01))
-- MAGIC print(f"Target Devices by stage after rebalance: {target_device_counts_by_stage}")
-- MAGIC
-- MAGIC # Set to True to unassign all gateways (except for stage 1000) - ONLY USE THIS to perform a full rebalance from scratch,
-- MAGIC # which generally you don't want to do. A use case can be if the criteria for balancing (e.g. activity criteria) have changed drastically.
-- MAGIC if dbutils.widgets.get("reassign_all_devices") == "True":
-- MAGIC   print("WARNING - unassigning all gateways (starting from scratch) - make sure you actually want this")
-- MAGIC   unassign_devices([device for device in devices if device.get_rollout_stage_id() not in config["reassign_all_devices_stage_exclusion"]], current_device_count_by_stage)
-- MAGIC   print(f"Devices by stage after unnassign: {current_device_count_by_stage}")
-- MAGIC
-- MAGIC if has_activity_logic:
-- MAGIC # Find devices currently assigned to 1000s stages that have become inactive
-- MAGIC # and unassign them to make room for new active devices.
-- MAGIC   assigned_inactive_devices = [device for device in devices if device.is_assigned()
-- MAGIC                                and device.get_activity() == "inactive"
-- MAGIC                                and device.get_rollout_stage_id() is not None
-- MAGIC                                and device.get_rollout_stage_id() >=1000
-- MAGIC                                and device.get_rollout_stage_id() not in config["reassign_all_devices_stage_exclusion"]]
-- MAGIC   print(f"Unassigning {len(assigned_inactive_devices)} inactive devices")
-- MAGIC   unassign_devices(assigned_inactive_devices, current_device_count_by_stage)
-- MAGIC
-- MAGIC # Find active devices in the inactive devices stage and unassign them
-- MAGIC # to distribute to either 1000s or escalated stages.
-- MAGIC   active_devices_in_inactive_stage = [device for device in devices if device.is_assigned()
-- MAGIC                                 and device.get_activity() != "inactive"
-- MAGIC                                 and device.get_rollout_stage_id() == config["inactive_devices_stage"]]
-- MAGIC   print(f"Unassigning {len(active_devices_in_inactive_stage)} active devices in the inactive devices stage")
-- MAGIC   unassign_devices(active_devices_in_inactive_stage, current_device_count_by_stage)
-- MAGIC
-- MAGIC # Handle red/yellow sentiment devices
-- MAGIC redDevices = devicesWithSentiment(devices, "red")
-- MAGIC yellowDevices = devicesWithSentiment(devices, "yellow")
-- MAGIC print(f"Red count {len(redDevices)} is {len(redDevices)/all_devices_count * 100:.2f}% of the total devices")
-- MAGIC print(f"Yellow count {len(yellowDevices)} is {len(yellowDevices)/all_devices_count * 100:.2f}% of the total devices")
-- MAGIC ryDevices = redDevices + yellowDevices
-- MAGIC print(f"Unassigning {len(ryDevices)} yellow/red devices")
-- MAGIC unassign_devices(ryDevices, current_device_count_by_stage)
-- MAGIC if has_activity_logic:
-- MAGIC   activeRyDevices = [device for device in ryDevices if device.get_activity() != "inactive"]
-- MAGIC   # Sort by rating (red first), then org_id so that we can assign all devices of an org to the same stage.
-- MAGIC   activeRyDevices.sort(key=lambda x: (x.get_csm_rating(), x.get_org_id()), reverse=False)
-- MAGIC else:
-- MAGIC   ryDevices.sort(key=lambda x: (x.get_csm_rating(), x.get_org_id()), reverse=False)
-- MAGIC
-- MAGIC # Make room in the stages we want R/Y devices in. Reverse the target_device_counts_by_stage to fill latest stages (100s) first,
-- MAGIC # as this is where we want to put escalated customers into.
-- MAGIC if has_activity_logic:
-- MAGIC   stages_needed_to_fit_ry_devices = stagesToFitDevices(len(activeRyDevices), target_device_counts_by_stage, reverse_order=True)
-- MAGIC   print(f"{len(activeRyDevices)} active Red/Yellow devices will be put into stages: {stages_needed_to_fit_ry_devices}")
-- MAGIC   num_ry_devices_left_to_remove = len(activeRyDevices)
-- MAGIC else:
-- MAGIC   stages_needed_to_fit_ry_devices = stagesToFitDevices(len(ryDevices), target_device_counts_by_stage, reverse_order=True)
-- MAGIC   print(f"{len(ryDevices)} Red/Yellow devices will be put into stages: {stages_needed_to_fit_ry_devices}")
-- MAGIC   num_ry_devices_left_to_remove = len(ryDevices)
-- MAGIC print("Making room in the stages if necessary.")
-- MAGIC for stage in stages_needed_to_fit_ry_devices:
-- MAGIC   num_ry_devices_left_to_remove = removeNumberOfDevicesFromStages(devices, num_ry_devices_left_to_remove, stage, current_device_count_by_stage, target_device_counts_by_stage)
-- MAGIC print(f"Devices by stage after making room for RY devices: {current_device_count_by_stage}")
-- MAGIC
-- MAGIC # R/Y devices move as an org, so move them using a method that does so by org
-- MAGIC if has_activity_logic:
-- MAGIC   moved = moveUnassignedDevicesIntoStagesByOrg(activeRyDevices, current_device_count_by_stage, target_device_counts_by_stage, reverse_order=True)
-- MAGIC   print(f"Moved {moved} unassigned active RY devices into stages.")
-- MAGIC   print(f"Devices by stage after active RY devices moved: {current_device_count_by_stage}")
-- MAGIC else:
-- MAGIC   moved = moveUnassignedDevicesIntoStagesByOrg(ryDevices, current_device_count_by_stage, target_device_counts_by_stage, reverse_order=True)
-- MAGIC   print(f"Moved {moved} RY devices into stages.")
-- MAGIC   print(f"Devices by stage after RY devices moved: {current_device_count_by_stage}")
-- MAGIC
-- MAGIC # Any stages with a device count over the target should be reduced down to the target (unless R/Y stage)
-- MAGIC unassigned = removeDevicesFromOverfilledStages(devices, current_device_count_by_stage, target_device_counts_by_stage)
-- MAGIC print(f"Unassigned {unassigned} devices from overfilled stages.")
-- MAGIC print(f"Devices by stage after removing: {current_device_count_by_stage}")
-- MAGIC
-- MAGIC if has_activity_logic:
-- MAGIC   # Move all the remaining devices into stages if possible
-- MAGIC   # 80% of high-activity
-- MAGIC   target_device_counts_by_stage_high_activity = OrderedDict()
-- MAGIC   for stage in target_device_counts_by_stage:
-- MAGIC     target_device_counts_by_stage_high_activity[stage] = int(target_device_counts_by_stage[stage] * 0.80)
-- MAGIC   moved = moveUnassignedDevicesIntoStagesByActivity(devices, current_device_count_by_stage, target_device_counts_by_stage, target_device_counts_by_stage_high_activity, activity_requirement="high")
-- MAGIC   print(f"Moved {moved} high-activity (and non-yellow/red) unassigned devices into stages to balance them.")
-- MAGIC   print("Devices by stage after high-activity rebalance:")
-- MAGIC   printCountsWithPercent(current_device_count_by_stage, all_devices_count, active_devices_count)
-- MAGIC
-- MAGIC   # 20% of low-activity
-- MAGIC   moved = moveUnassignedDevicesIntoStages(devices, current_device_count_by_stage, target_device_counts_by_stage, activity_requirement="low")
-- MAGIC   print(f"Moved {moved} low-activity (and non-yellow/red) unassigned devices into stages to balance them.")
-- MAGIC   print("Devices by stage after low-activity rebalance:")
-- MAGIC   printCountsWithPercent(current_device_count_by_stage, all_devices_count, active_devices_count)
-- MAGIC
-- MAGIC   # move unassigned (includes inactive) devices to their reserved stage, so that we don't keep any device unassigned.
-- MAGIC   inactive_devices = [device for device in devices if not device.is_assigned()]
-- MAGIC   print(f"Moving {len(inactive_devices)} inactive unassigned devices into stage {config['inactive_devices_stage']}.")
-- MAGIC   for device in inactive_devices:
-- MAGIC     move_device_to_stage(device, current_device_count_by_stage, config["inactive_devices_stage"])
-- MAGIC else:
-- MAGIC   moved = moveUnassignedDevicesIntoStages(devices, current_device_count_by_stage, target_device_counts_by_stage, activity_requirement=None)
-- MAGIC   print("Devices by stage after inactive rebalance:")
-- MAGIC   printCountsWithPercent(current_device_count_by_stage, all_devices_count, all_devices_count)
-- MAGIC
-- MAGIC print(f"(To compare) Target Devices by stage expected: {target_device_counts_by_stage}")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Generate output and visualizations

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, LongType, StringType, BooleanType
-- MAGIC
-- MAGIC schema = StructType([
-- MAGIC   StructField("_org_id", LongType(), False),
-- MAGIC   StructField("_product_id", IntegerType(), False),
-- MAGIC   StructField("_device_id", LongType(), False),
-- MAGIC   StructField("_gateway_id", LongType(), False),
-- MAGIC   StructField("_rollout_stage_id", IntegerType(), True),
-- MAGIC   StructField("_csm_rating", StringType(), False),
-- MAGIC  # activity is not present for legacy monitors, so it's nullable
-- MAGIC   StructField("_activity", StringType(), True),
-- MAGIC ])
-- MAGIC
-- MAGIC newDf = spark.createDataFrame(devices, schema=schema)
-- MAGIC for column in newDf.columns:
-- MAGIC   newDf = newDf.withColumnRenamed(column, column[1:]) # remove '_' prefix
-- MAGIC
-- MAGIC newDf.createOrReplaceTempView("balanced_rollout_stages")

-- COMMAND ----------

-- output to table for reference
-- create or replace table firmware_dev.lucas_balanced_rollout_stages_2023_11_17 using delta as (
--   select * from balanced_rollout_stages
-- )
-- to load from pre-existing table instead of above
-- create or replace temp view balanced_rollout_stages as (
--   select * from firmware_dev.lucas_balanced_rollout_stages_2023_11_17
-- );

-- COMMAND ----------

-- merge current stage balance and proposed one
create or replace temp view current_and_new as (
  select
    current.gateway_id,
    current.product_id,
    current.csm_rating,
    current.org_id,
    coalesce(current.rollout_stage_id, 0) as current_rollout_stage_id,
    coalesce(new.rollout_stage_id, 0) as new_rollout_stage_id
  from allActiveDevicesWithStagesAndSentiment as current
  left join balanced_rollout_stages as new
    on new.gateway_id = current.gateway_id
);

-- COMMAND ----------

--MAGIC %python
--MAGIC
--MAGIC delta_table_name = "firmware_dev.rollout_stage_rebalance_output"
--MAGIC
--MAGIC run_identifier = spark.sql(f"select max(run_id) as max_run_id from {delta_table_name}").collect()[0].max_run_id
--MAGIC run_identifier += 1
--MAGIC
--MAGIC spark.sql(f"insert into table {delta_table_name} select *, {run_identifier} as run_id, current_timestamp() as time, \"{dbutils.widgets.get('product_name')}\" as product_name from current_and_new")

-- COMMAND ----------

-- DBTITLE 1,Gateway movement during rebalancing (changes only)
select current_rollout_stage_id, new_rollout_stage_id, count(gateway_id) as value from current_and_new
where current_rollout_stage_id != new_rollout_stage_id
group by 1,2 order by 1,2

-- COMMAND ----------

-- DBTITLE 1,Gateway movement during rebalancing (all - including no movement)
select current_rollout_stage_id, new_rollout_stage_id, count(gateway_id) as value from current_and_new
group by 1,2 order by 1,2

-- COMMAND ----------

-- DBTITLE 1,New stage counts (after rebalancing)
select coalesce(rollout_stage_id, "unassigned") as rollout_stage, count(gateway_id) as count from balanced_rollout_stages group by 1 order by int(rollout_stage)

-- COMMAND ----------

-- DBTITLE 1,Previous versus new stage counts (0 means unassigned)
select
  coalesce(b.rollout_stage_id, 0) as rollout_stage_id,
  coalesce(a.count, 0) as previous_count,
  coalesce(b.count, 0) as new_count
  from  (
    select coalesce(rollout_stage_id, 0) as rollout_stage_id, count(gateway_id) as count from balanced_rollout_stages group by 1
  ) as b
  full outer join (
    select coalesce(rollout_stage_id, 0) as rollout_stage_id, count(gateway_id) as count from allActiveDevicesWithStages group by 1
  ) as a
    on a.rollout_stage_id = b.rollout_stage_id
  order by 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Checking rules we designed against are respected

-- COMMAND ----------

-- DBTITLE 1,Prepare CSV export - only keep gateways where new rollout stage != current
create or replace temp view new_rollout_stages_export as (
  select
    new.org_id, new.product_id, new.gateway_id, coalesce(new.rollout_stage_id, 0) as rollout_stage_id
  from balanced_rollout_stages as new
  left join allActiveDevicesWithStagesAndSentiment as current
    on new.gateway_id = current.gateway_id and new.org_id = current.org_id
  where current.rollout_stage_id != new.rollout_stage_id
    or current.rollout_stage_id is null or new.rollout_stage_id is null
);
select count(*) as num_gateways_to_rebalance from new_rollout_stages_export

-- COMMAND ----------

-- all gateways should have been assigned to a stage
select * from new_rollout_stages_export where rollout_stage_id is null or rollout_stage_id = 0

-- COMMAND ----------

-- Stage 1000 is handled separately: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5420369/VG28+Release+Stage+1000+Rebalance
select * from current_and_new where current_rollout_stage_id = 1000 and new_rollout_stage_id != 1000 and csm_rating not in ("Yellow", "Red")

-- COMMAND ----------

-- It's ok to remove escalated customer devices from stage 1000
select * from current_and_new where current_rollout_stage_id = 1000 and new_rollout_stage_id != 1000 and csm_rating in ("Yellow", "Red")

-- COMMAND ----------

-- all devices belonging to escalated orgs must be in the 100s stages.
select * from balanced_rollout_stages where csm_rating in ("yellow", "red") and rollout_stage_id not in (100,200,300,400,500,600)

-- COMMAND ----------

-- For escalated orgs, all devices for that org must belong to the same rollout stage
select org_id, count(distinct rollout_stage_id) as stage_count
from balanced_rollout_stages
where csm_rating in ("yellow", "red")
group by org_id
having stage_count > 1

-- COMMAND ----------

-- The 1000s stages must never include escalated orgs
select * from balanced_rollout_stages where csm_rating in ("yellow", "red") and rollout_stage_id >= 1000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Output

-- COMMAND ----------

-- DBTITLE 1,Preview truncated results. Full results are saved to S3 by the next command
select * from new_rollout_stages_export

-- COMMAND ----------

-- DBTITLE 1,Results will be saved to s3 in csv format. copy the path output here to use as input to the python script
-- MAGIC %python
-- MAGIC from datetime import datetime
-- MAGIC import os
-- MAGIC
-- MAGIC s3_bucket = ""
-- MAGIC region = os.getenv("AWS_REGION")
-- MAGIC if region == "eu-west-1":
-- MAGIC   s3_bucket = "s3://samsara-eu-databricks-workspace"
-- MAGIC elif region == "us-west-2":
-- MAGIC   s3_bucket = "s3://samsara-databricks-workspace"
-- MAGIC else:
-- MAGIC   raise ValueError("unknown region, won't write to S3")
-- MAGIC
-- MAGIC s3_output_path = f"{s3_bucket}/firmware/rollout_stage_rebalance_for_default_program/{dbutils.widgets.get('product_name')}_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.csv"
-- MAGIC
-- MAGIC spark.sql("select * from new_rollout_stages_export").write.option("header", True).format("csv").save(s3_output_path)
-- MAGIC
-- MAGIC print(f"Stage rebalance file saved to {s3_output_path}")
-- MAGIC print()
-- MAGIC print()
-- MAGIC print(f"Generate the graphql mutation using the following command:")
-- MAGIC print()
-- MAGIC print(f"(\n\tcd ~/co/backend &&\n\tpython python3/samsaradev/firmware/update_rollout_stages.py \\\n\t\t--input_csv {s3_output_path} \\\n\t\t--program <PROGRAM_ID> \\\n\t\t<gateway|org>\n)")
-- MAGIC print()
-- MAGIC print("------")
-- MAGIC print(f"Wrote rollout stage rebalance output to table. Your run identifier is: {run_identifier}.")
-- MAGIC print("You can use this run id as input to the rebalance visualization notebook:")
-- MAGIC print("  * NA: https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#notebook/957455983151449")
-- MAGIC print("  * EU: https://samsara-dev-eu-west-1.cloud.databricks.com/?o=6992178240159315#notebook/4033060600739020")
