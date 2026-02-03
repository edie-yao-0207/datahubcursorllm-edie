-- Add serials that occur only ones in the gateway_summary table
create or replace temp view single_life_summaries as (
  select x.name, x.serial, collect_set(x.sam_number) as sam_numbers, x.ship_date, x.first_heartbeat_date, x.last_heartbeat_date, coalesce(x.return_date, scrf.screen_date) as return_date
  -- screened date used in the absence of a return date, Only serials that occur in one SAM, so no need SAM in screen data
  from hardware.life_summary as x
  join (select name, serial, count(*) as cnt from hardware.life_summary group by name, serial having cnt == 1) as y
  on x.serial = y.serial
  join platops.org_name_to_samnumber as psm
  on x.sam_number = psm.sam_number
  left join (select serial, min(date_screened) as screen_date
            from hardware.initial_screening_results
            group by serial) as scrf
  on x.serial = scrf.serial
  where psm.internal_type == 0
  group by x.name, x.serial, x.ship_date, x.first_heartbeat_date, x.last_heartbeat_date, x.return_date, scrf.screen_date
);


-- All gateways that occur many times in the table
-- gather all dates and types and order by date, SAM & date_type, 
--so first preference is for order of dates, 
--second for all dates within sam to be grouped together and
--third for ship, first, last, return date orfer
-- Every ship date <--> return_date is a one group and all dates are merged within it.

create or replace temp view multiple_life_summaries as (
with all_data as (
  select x.name, x.serial, x.sam_number, x.ship_date, x.first_heartbeat_date, x.last_heartbeat_date, x.return_date
  from hardware.life_summary as x
  join (select name, serial, count(*) as cnt from hardware.life_summary group by name, serial having cnt > 1) as y
  on x.serial = y.serial
  join platops.org_name_to_samnumber as psm
  on x.sam_number = psm.sam_number
  where psm.internal_type == 0
),

ship_dates as ( -- get shipped dates
select name, sam_number, serial, 'a_ship_date' as date_type, ship_date as date
from all_data
where ship_date is not null),

first_heartbeat_dates as ( -- all first_heartbeat_date
select name, sam_number, serial, 'b_first_heartbeat_date' as date_type, first_heartbeat_date as date
from all_data
where first_heartbeat_date is not null),

last_heartbeat_dates as ( -- all last_heartbeat_date
select name, sam_number, serial, 'c_last_heartbeat_date' as date_type, last_heartbeat_date as date
from all_data
where last_heartbeat_date is not null),

return_dates as (
select name, sam_number, serial, 'd_return_date' as date_type, return_date as date
from all_data
where return_date is not null),

screen_dates as ( -- all screened dates
  select dp.name, null as sam_number, scrf.serial, 'e_screen_date' as date_type, scrf.screen_date as date
  from (select name, serial, count(*) as cnt from hardware.life_summary group by name, serial having cnt > 1) as y
  join (select serial, min(date_screened) as screen_date
            from hardware.initial_screening_results
            group by serial) as scrf
  on y.serial = scrf.serial
  join ((select serial, product_id
         from productsdb.gateways)
         union (select serial, product_id
         from productsdb.widgets)) as g
  on g.serial = scrf.serial
  join definitions.products as dp
  on g.product_id = dp.product_id
),

initial_group_data as (
select *, sum(case when date_type == 'a_ship_date' then 1 else 0 end) over(partition by serial order by date, sam_number, date_type) as group_number 
-- every ship date becomes a new group
-- order by sam_number so that we consider them as one group first
-- order by date so ship - first - last and then return order is kept.
      from ((select *
            from ship_dates)
            union
            (select *
            from first_heartbeat_dates)
            union
            (select *
            from last_heartbeat_dates)
            union
            (select *
            from return_dates)
            union
            (select *
            from screen_dates))
),

final_data as ( -- Handles conditions when first hb is before ship 
      select name, serial,  date_type, sam_number, date,
                  case when date_type == 'b_first_heartbeat_date' and next_date_type == 'a_ship_date' then next_group_number
                  else group_number end as group_number
      from (select *,
                  lead(date_type) over(partition by serial order by date) as next_date_type,
                  lead(group_number) over(partition by serial order by date) as next_group_number
            from initial_group_data)
),

all_sam_numbers as (
      select name, serial,  group_number, collect_set(sam_number) as sam_numbers
      from final_data
      group by name, serial,  group_number
)

select x.name, x.serial, y.sam_numbers, x.a_ship_date as ship_date, x.b_first_heartbeat_date as first_heartbeat_date, x.c_last_heartbeat_date as last_heartbeat_date, coalesce(x.d_return_date, x.e_screen_date) as return_date
from (
      select *
      from (
      select name, serial, date_type, group_number, case when date_type = 'a_ship_date' or date_type = 'b_first_heartbeat_date' then min(date) else max(date) end as date
      from final_data
      group by name, serial, date_type, group_number
      )
      pivot( min(date)
      FOR date_type in ('a_ship_date', 'b_first_heartbeat_date', 'c_last_heartbeat_date', 'd_return_date', 'e_screen_date'))
      ) as x
join all_sam_numbers as y
on x.name = y.name
and x.serial = y.serial
and x.group_number = y.group_number
);

create or replace temp view multiple_life_summaries_clean as ( 
    -- must have both first & last then complete. 
    -- even if it is complete but if it's predecessor or successor is incomplete then incomplete - need to be merged
select name, serial, sam_numbers, ship_date, first_heartbeat_date, last_heartbeat_date, return_date
from (select *, lag(is_it_complete) over (partition by serial order by ship_date) as lag_complete, lead(is_it_complete) over (partition by serial order by ship_date) as lead_complete
    from (select *, case when ((first_heartbeat_date is null and last_heartbeat_date is null) or (first_heartbeat_date is not null and last_heartbeat_date is not null)) then 1 else 0 end as is_it_complete
        from multiple_life_summaries)
)
where is_it_complete = 1 and (lag_complete = 1 or lag_complete is null) and (lead_complete = 1 or lead_complete is null)
);

-- Some order of dates are weird, eg ship, first, ship, last, return order of events
create or replace temp view multiple_life_summaries_messy as ( -- if one is avaiable and the other is missing, it needs further grouping. They always occur in pairs.
select name, serial, collect_set(sam_numbers) as sam_numbers, min(ship_date) as ship_date, min(first_heartbeat_date) as first_heartbeat_date, max(last_heartbeat_date) as last_heartbeat_date, max(return_date) as return_date
from (select name, serial, explode(sam_numbers) as sam_numbers, ship_date,  first_heartbeat_date, last_heartbeat_date, return_date 
    from (select *
      from multiple_life_summaries
      except
      select *
      from multiple_life_summaries_clean)
)
group by name, serial
);

create or replace temp view combined_life_summaries as (
  select current_date() as date, * from multiple_life_summaries_clean
  union
  select current_date() as date, * from multiple_life_summaries_messy
  union
  select current_date() as date, * from single_life_summaries
);


insert into hardware_analytics.life_summary
select date, name, serial, sam_numbers, ship_date, first_heartbeat_date, last_heartbeat_date, return_date
from combined_life_summaries;