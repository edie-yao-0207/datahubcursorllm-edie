create or replace temp view samnumber_orgs_updates as (
select
  s.sam_number as sam_number,
  o.id as org_id
from clouddb.organizations o
join clouddb.org_sfdc_accounts osa on osa.org_id = o.id
join clouddb.sfdc_accounts s on s.id = osa.sfdc_account_id
);

create table if not exists dataprep.samnumber_orgs
using delta
as
(select * from samnumber_orgs_updates);

merge into dataprep.samnumber_orgs as target
using samnumber_orgs_updates as updates
on target.org_id = updates.org_id
and target.sam_number = updates.sam_number
when matched then update set *
when not matched then insert *;


-- UPDATE METADATA --------
ALTER TABLE dataprep.samnumber_orgs
SET TBLPROPERTIES ('comment' = 'A table that maps SAM numbers to dashboard Org IDs. Note that a SAM number may have multiple org IDs; in this case there will be multiple rows for that SAM number in this view.');

ALTER TABLE dataprep.samnumber_orgs
CHANGE sam_number
COMMENT 'A SAM number is a unique identifier for a Samsara customer. Each customer, in theory, should have a single SAM number. This SAM number can be found in the customer account’s page in Salesforce.';

ALTER TABLE dataprep.samnumber_orgs
CHANGE org_id
COMMENT 'The Samsara cloud dashboard ID that the data belongs to';

