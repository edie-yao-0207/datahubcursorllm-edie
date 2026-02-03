with orgs as (
    select
        organizations.id as id,
        organizations.name as name,
        organizations.internal_type,
        (
            select count(*)
            from productsdb.devices
            where devices.org_id = organizations.id
        ) as device_count,
        case
          when customer_metadata.industry in ('Building Materials', 'Commercial & Residential Construction', 'Construction', 'HVAC / Electrical / Plumbing','Machinery','Plumbing/HVAC')
            then 'Construction'
          when customer_metadata.industry in ('Education - Higher Ed','Education - K-12','Educational Services')
            then 'SLED'
          when customer_metadata.industry in ('Field Services','Landscaping','Movers','Pest Control','Waste Management','Waste Treatment, Environmental Services & Recycling')
            then 'Field Services'
          when customer_metadata.industry in ('Food & Beverage')
            then 'Food & Beverage'
          when customer_metadata.industry in ('Government','Government - Airport','Government - Federal','Government - Local','Government - Outside US/CA','Government - State',
            'Government - State & Local' )
            then 'SLED'
          when customer_metadata.industry in ('HC&SA','Healthcare','Healthcare Delivery','Hospital & Health Care','Medical Devices','Health Care & Social Assistance','Pharmaceuticals' )
            then 'Health Care and Social Assistance'
          when customer_metadata.industry in ('Automotive','Chemicals','Consumer Products','Electrical/Electronic Manufacturing','Manufacturing' )
            then 'Manufacturing'
          when customer_metadata.industry in ('Aggregates, Concrete & Cement','Biotechnology','Metals & Minerals','Mining','Mining / Metals','Mining, Quarrying, Oil and Gas','Mining,
            Quarrying, Oil & Gas','Oil & Energy','Oil & Gas' )
            then 'Other'
          when customer_metadata.industry in('Passenger Transit','Rail, Bus & Taxi','Transportation - Passenger Transit','Travel Agencies & Services' )
            then 'Passenger Transit'
          when customer_metadata.industry in ( 'Car & Truck Rental','Commerce / Distribution','Freight & Logistics Services','Industry.transportation.freight','Logistics','Towing',
            'Transporation & Warehousing','Transportation','Transportation & Warehousing','Transportation and Warehousing','Transportation/Trucking/Railroad' )
            then 'Transportation and Warehousing'
          when customer_metadata.industry in ( 'Electrical','Energy','Energy, Utilities & Waste Treatment General','Renewables & Environment','Utilities','Utility','Water' )
            then 'Utilities'
          when customer_metadata.industry in ( 'Retail','Retail General','Retail Trade','Wholesale','Wholesale Trade' )
            then 'Wholesale / Retail'
          else 'Other'
        end as industry
    from clouddb.organizations
    join dataprep.customer_metadata on organizations.id = customer_metadata.org_id
)
select
    id,
    name,
    device_count,
    internal_type,
    case
        when (device_count < 100) then 'small'
        when (device_count < 500) then 'medium'
        else 'large'
     end as size_class,
     industry
from orgs
    where device_count > 0

