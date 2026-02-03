(
  select string(cell_id) as cell_id
  from clouddb.org_cells
)
union
(
  select 'all' as cell_id
)
order by cell_id desc
