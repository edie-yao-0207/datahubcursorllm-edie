-- Databricks notebook source

create or replace temporary view gateways_with_upper_serial as
(
  select
  org_id,
  device_id,
  product_id,
  upper(replace(gateways.serial,'-','')) as upper_serial
from productsdb.gateways
);

create or replace temporary view devices_with_upper_camera_serial as
(
  select
  id,
  product_id,
  org_id,
  upper(replace(devices.camera_serial,'-','')) as upper_camera_serial
from productsdb.devices
);

create or replace temporary view raw_cm_linked_vgs as
(
  select
  gateways.org_id,
  devices.id as vg_device_id,
  devices.product_id as product_id,
  gateways.device_id as linked_cm_id,
  orgs.name as org_name,
  case
    when orgs.internal_type = 1 then 'Internal'
    when orgs.internal_type = 0 then 'Customer'
    else 'Error'
  end as org_type,
  gateways.product_id as cm_product_id
from gateways_with_upper_serial gateways
  join devices_with_upper_camera_serial devices on
    upper_serial = upper_camera_serial and
    devices.org_id = gateways.org_id
  join clouddb.organizations as orgs on gateways.org_id = orgs.id
where
  orgs.quarantine_enabled != 1
);

create or replace temporary view cm_linked_vgs as
(
  select *
from raw_cm_linked_vgs
where cm_product_id in (43, 44, 155, 167) -- 155, 167 are Brigid Dual and Brigid Single, respectively
);

-- COMMAND ----------

create table
if not exists dataprep_safety.cm_linked_vgs
using delta as
select *
from cm_linked_vgs

-- COMMAND ----------

insert overwrite
table dataprep_safety.cm_linked_vgs
(
  select *
from cm_linked_vgs
);

-- UPDATE METADATA --------
ALTER TABLE dataprep_safety.cm_linked_vgs
SET TBLPROPERTIES ('comment' = 'Table contains the VG<->CM device ID pairs for CM 3xs and includes additional data like the org name, type and CM product ID');

ALTER TABLE dataprep_safety.cm_linked_vgs
CHANGE org_id
COMMENT 'The Samsara Cloud dashboard ID that the data belongs to';

ALTER TABLE dataprep_safety.cm_linked_vgs
CHANGE vg_device_id
COMMENT 'The device ID of the VG in this VG<->CM pairing.';

ALTER TABLE dataprep_safety.cm_linked_vgs
CHANGE product_id
COMMENT 'The product ID of the VG in this VG<->CM pairing. The product ID mapping can be found in the products.go file in the codebase';

ALTER TABLE dataprep_safety.cm_linked_vgs
CHANGE linked_cm_id
COMMENT 'The device ID of the CM in this VG<->CM pairing.';

ALTER TABLE dataprep_safety.cm_linked_vgs
CHANGE org_name
COMMENT 'The name of the organization that these devices belong to.';

ALTER TABLE dataprep_safety.cm_linked_vgs
CHANGE org_type
COMMENT 'The organization type that these devices belong to. This would be Internal, or Customer.';

ALTER TABLE dataprep_safety.cm_linked_vgs
CHANGE cm_product_id
COMMENT 'The product ID of the CM in this VG<->CM pairing. These should just be 43 (CM32) or 44 (CM33).';
