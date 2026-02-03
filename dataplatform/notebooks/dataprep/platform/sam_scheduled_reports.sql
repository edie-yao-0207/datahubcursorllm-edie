create or replace temp view org_scheduled_reports as (
  select
    o.id as org_id,
    count(distinct sr.id) as num_scheduled_reports
  from reportconfigdb_shards.scheduled_report_plans sr
  left join clouddb.groups g on sr.group_id = g.id
  left join clouddb.organizations o on g.organization_id = o.id
  where o.internal_type <> 1
  group by o.id
);

create or replace temp view sam_scheduled_reports as (
  select
    current_date() as date,
    s.sam_number,
    sum(num_scheduled_reports) as num_scheduled_reports
  from org_scheduled_reports sr
  left join clouddb.org_sfdc_accounts o on sr.org_id = o.org_id
  left join clouddb.sfdc_accounts s on o.sfdc_account_id = s.id
  group by s.sam_number
);

create table if not exists dataprep.sam_scheduled_reports using delta partitioned by (date) (
  select
    *
  from sam_scheduled_reports
);

merge into dataprep.sam_scheduled_reports as target
using sam_scheduled_reports as updates
on target.date = updates.date
and target.sam_number = updates.sam_number
when matched then update set *
when not matched then insert *;

alter table dataprep.sam_scheduled_reports set tblproperties ('comment' = 'A table with a daily snapshot of the number of scheduled reports a customer has scheduled.');
alter table dataprep.sam_scheduled_reports change date comment "The calendar date in 'YYYY-mm-dd' format";
alter table dataprep.sam_scheduled_reports change sam_number comment 'Samnumber is a unique id that ties customer accounts together in Netsuite, Salesforce, and Samsara cloud dashboard.';
alter table dataprep.sam_scheduled_reports change num_scheduled_reports comment 'The number of scheduled reports the sam_number had configured on this date';
