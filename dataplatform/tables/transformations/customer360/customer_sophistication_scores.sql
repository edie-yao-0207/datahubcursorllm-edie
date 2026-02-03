WITH products_owned_sam AS (
    SELECT
        sam_number,
        MAX(CASE WHEN product LIKE '%LIC-VG%' THEN true ELSE false END) AS owns_vg,
        MAX(CASE WHEN product LIKE '%LIC-AG%'  THEN true ELSE false END) AS owns_ag,
        MAX(CASE WHEN product LIKE '%LIC-CM%' THEN true ELSE false END) AS owns_cm
    FROM dataprep.devices_sold ds
    GROUP BY 
        sam_number
),

num_active_vehicles_org AS (
    SELECT 
        org_id,
        COUNT(DISTINCT (device_id, org_id)) AS num_vehicles
    FROM dataprep.active_devices
    WHERE 
        date >= date_sub(current_date(), 30) AND
        trip_count > 0 AND
        product_id IN (7, 17, 24, 35, 53) -- only VG device types
    GROUP BY 
        org_id
),

num_active_vehicles_sam AS (
    SELECT
        sam_number,
        sum(num_vehicles) AS num_vehicles
    FROM num_active_vehicles_org ad
    LEFT JOIN clouddb.org_sfdc_accounts o ON ad.org_id = o.org_id
    LEFT JOIN clouddb.sfdc_accounts s ON o.sfdc_account_id = s.id
    GROUP BY 
        sam_number
),

num_total_vehicles_org AS (
    SELECT 
        org_id,
        COUNT(DISTINCT (id, org_id)) AS num_total_vehicles
    FROM productsdb.devices
    WHERE 
        product_id IN (7, 17, 24, 35, 53) -- only VG device types
    GROUP BY 
        org_id
),

num_total_vehicles_sam AS (
    SELECT
        sam_number,
        SUM(num_total_vehicles) AS num_total_vehicles
    FROM num_total_vehicles_org ad
    LEFT JOIN clouddb.org_sfdc_accounts o ON ad.org_id = o.org_id
    LEFT JOIN clouddb.sfdc_accounts s ON o.sfdc_account_id = s.id
    GROUP BY 
        sam_number
),

as_of_metrics AS (
    SELECT
        c360.sam_number,
        MAX(num_scheduled_reports) AS num_scheduled_reports,
        MAX(num_routes) AS num_routes,
        MAX(num_addresses) AS num_addresses,
        MAX(num_alerts) AS num_alerts,
        MAX(num_roles) AS num_roles,
        MAX(num_tags) AS num_tags,
        MAX(safety_score_configured) AS safety_score_configured,
        MAX(num_vg_purchased) AS num_vg_purchased,
        MAX(csm_name) AS csm_name,
        MAX(ae_name) AS ae_name
    FROM customer360.customer_360 c360
    LEFT JOIN customer360.customer_360_snapshot c360s ON
      c360.sam_number = c360s.sam_number
    WHERE 
        date > date_sub(current_date(), 30)
    GROUP BY
        c360.sam_number
),

agg_metrics AS (
    SELECT
        c.sam_number,
        SUM(c.num_api_calls) AS num_api_calls,
        SUM(c.num_route_loads) AS num_route_loads,
        SUM(reviewed + coached + recognized + dismissed) AS num_handled_events,
        SUM(needs_review + needs_coaching + needs_recognition + reviewed + coached + recognized + dismissed) AS num_events,
        SUM(num_admin_app_loads) AS num_admin_app_loads
    FROM customer360.customer_360 c
    WHERE 
        c.date > date_sub(current_date(), 30)
    GROUP BY 
        c.sam_number
),

vg_customers AS (
    SELECT 
        DISTINCT sam_number
    FROM dataprep.devices_sold
    WHERE product LIKE '%VG%'
),

deactivated_accounts AS (
    SELECT 
        DISTINCT sam_number
    FROM clouddb.sfdc_accounts s
    LEFT JOIN clouddb.org_sfdc_accounts os ON os.sfdc_account_id = s.id
    LEFT JOIN clouddb.organizations o ON o.id = os.org_id
    WHERE o.quarantine_enabled <> 1
),

customers AS (
  SELECT
    sam_number,
    MAX(name) AS name,
    MAX(segment) AS segment,
    MAX(industry) AS industry,
    MAX(csm_name) AS csm_name,
    MAX(ae_name) AS ae_name,
    MAX(lifetime_acv) AS lifetime_acv,
    MAX(csm_tier) AS csm_tier
  FROM dataprep.customer_metadata
  GROUP BY sam_number
),

all_metrics AS (
  SELECT 
      sf.sam_number, 
      sf.name,
      num_vehicles,
      num_scheduled_reports,
      num_routes,
      num_addresses,
      num_alerts,
      num_roles,
      safety_score_configured,
      num_api_calls,
      num_route_loads,
      num_handled_events,
      num_events,
      num_tags,
      num_admin_app_loads,
      num_total_vehicles, 
      num_total_vehicles / num_vg_purchased AS vg_installation_rate,
      sf.segment,
      CASE 
          WHEN sf.Industry IN ('Building Materials', 'Commercial & Residential Construction', 'Construction', 'HVAC / Electrical / Plumbing','Machinery','Plumbing/HVAC') 
            THEN 'Construction'
          WHEN sf.Industry IN ('Education - Higher Ed','Education - K-12','Educational Services') 
            THEN 'SLED'
          WHEN sf.Industry IN ('Field Services','Landscaping','Movers','Pest Control','Waste Management','Waste Treatment, Environmental Services & Recycling') 
            THEN 'Field Services'
          WHEN sf.Industry IN ('Food & Beverage') 
            THEN 'Food & Beverage'
          WHEN sf.Industry IN ('Government','Government - Airport','Government - Federal','Government - Local','Government - Outside US/CA','Government - State',
            'Government - State & Local' )
            THEN 'SLED'
          WHEN sf.Industry IN ('HC&SA','Healthcare','Healthcare Delivery','Hospital & Health Care','Medical Devices','Health Care & Social Assistance','Pharmaceuticals' )
            THEN 'Health Care and Social Assistance'
          WHEN sf.Industry IN ('Automotive','Chemicals','Consumer Products','Electrical/Electronic Manufacturing','Manufacturing' )
            THEN 'Manufacturing'
          WHEN sf.Industry IN ('Aggregates, Concrete & Cement','Biotechnology','Metals & Minerals','Mining','Mining / Metals','Mining, Quarrying, Oil and Gas','Mining, 
            Quarrying, Oil & Gas','Oil & Energy','Oil & Gas' )
            THEN 'Other'
          WHEN sf.Industry IN ('Passenger Transit','Rail, Bus & Taxi','Transportation - Passenger Transit','Travel Agencies & Services' )
            THEN 'Passenger Transit'
          WHEN sf.Industry IN ( 'Car & Truck Rental','Commerce / Distribution','Freight & Logistics Services','Industry.transportation.freight','Logistics','Towing',
            'Transporation & Warehousing','Transportation','Transportation & Warehousing','Transportation and Warehousing','Transportation/Trucking/Railroad' )
            THEN 'Transportation and Warehousing'
          WHEN sf.Industry IN ( 'Electrical','Energy','Energy, Utilities & Waste Treatment General','Renewables & Environment','Utilities','Utility','Water' )
            THEN 'Utilities'
          WHEN sf.Industry IN ( 'Retail','Retail General','Retail Trade','Wholesale','Wholesale Trade' )
            THEN 'Wholesale / Retail'
          ELSE 'Other' 
        END AS industry,
      sf.csm_name,
      sf.ae_name,
      sf.lifetime_acv,
      sf.csm_tier,
      o.owns_vg,
      o.owns_ag,
      o.owns_cm
  FROM customers sf
  LEFT JOIN vg_customers v ON sf.sam_number = v.sam_number
  LEFT JOIN as_of_metrics a ON sf.sam_number = a.sam_number
  LEFT JOIN agg_metrics b ON sf.sam_number = b.sam_number
  LEFT JOIN num_active_vehicles_sam c ON sf.sam_number = c.sam_number
  LEFT JOIN num_total_vehicles_sam t ON sf.sam_number = t.sam_number
  LEFT JOIN products_owned_sam o ON sf.sam_number = o.sam_number
  WHERE
      sf.sam_number IS NOT NULL
),

customer_sophistication_raw_scores AS (
  SELECT *,
    (
      CASE 
        WHEN num_addresses = 0 OR ISNULL(num_addresses) THEN 0
        WHEN num_addresses < 3 THEN 0.25 
        WHEN num_addresses < 17 THEN 0.5 
        WHEN num_addresses < 141 THEN 0.75
        ELSE 1.0
      END / (3/3)
    ) +
    (
      CASE
        WHEN num_alerts = 0 OR ISNULL(num_alerts) THEN 0
        WHEN num_alerts < 4 THEN 0.25
        WHEN num_alerts < num_api_calls THEN 0.5 
        WHEN num_alerts < 18 THEN 0.75
        ELSE 1.0
      END / (3/3)
    ) +
    (
      CASE
        WHEN num_api_calls = 0 OR ISNULL(num_api_calls) THEN 0
        WHEN num_api_calls < 1220 THEN 0.25
        WHEN num_api_calls < 3118 THEN 0.5
        WHEN num_api_calls < 9681 THEN 0.75
        ELSE 1.0
      END / (3/3)
    ) + 
    (
      CASE
        WHEN num_handled_events / num_events = 0 OR ISNULL(num_handled_events) THEN 0
        WHEN num_handled_events / num_events < 0.1 THEN 0.25
        WHEN num_handled_events / num_events < 0.25 THEN 0.5
        WHEN num_handled_events / num_events < 0.5 THEN 0.75
        ELSE 1.0
      END / (5/3)
    ) +
    (
      CASE
        WHEN num_roles = 0 OR ISNULL(num_roles) THEN 0
        WHEN num_roles < 1 THEN 0.25
        WHEN num_roles < 2 THEN 0.5
        WHEN num_roles < 3 THEN 0.75
        ELSE 1.0
      END / (3/3)
    ) +
    (
      CASE
        WHEN num_route_loads = 0 OR ISNULL(num_route_loads) THEN 0
        WHEN num_route_loads < 344 THEN 0.25
        WHEN num_route_loads < 879 THEN 0.5
        WHEN num_route_loads < 2175 THEN 0.75
        ELSE 1.0
      END / (5/3)
    ) +
    (
      CASE
        WHEN num_routes = 0 OR ISNULL(num_routes) THEN 0
        WHEN num_routes < 3 THEN 0.25
        WHEN num_routes < 12 THEN 0.5
        WHEN num_routes < 50 THEN 0.75
        ELSE 1.0
      END / (3/3)
    ) +
    (
      CASE
        WHEN num_scheduled_reports = 0 OR ISNULL(num_scheduled_reports) THEN 0
        WHEN num_scheduled_reports < 1 THEN 0.25
        WHEN num_scheduled_reports < 3 THEN 0.5
        WHEN num_scheduled_reports < 6 THEN 0.75
        ELSE 1.0
      END / (3/3)
    ) +
   (
      CASE
        WHEN num_tags = 0 OR ISNULL(num_tags) THEN 0
        WHEN num_tags < 2 THEN 0.25
        WHEN num_tags < 5 THEN 0.5
        WHEN num_tags < 12 THEN 0.75
        ELSE 1.0
      END / (3/3)
    ) +
    (
      CASE
        WHEN safety_score_configured THEN 1
        ELSE 0
      END / (3/3)
    ) +
    (
      CASE
        WHEN num_admin_app_loads = 0 OR ISNULL(num_admin_app_loads) THEN 0
        WHEN num_admin_app_loads < 21 THEN 0.25
        WHEN num_tags < 71 THEN 0.5
        WHEN num_tags < 208 THEN 0.75
        ELSE 1.0
      END / (3/3)
    ) +
    (
      CASE
        WHEN vg_installation_rate = 0 OR ISNULL(vg_installation_rate) THEN 0
        WHEN vg_installation_rate < 25 THEN 0.25
        WHEN vg_installation_rate < 50 THEN 0.5
        WHEN vg_installation_rate < 75 THEN 0.75
        ELSE 1.0
      END / (3/3)
    ) AS customer_sophistication_raw_score
  FROM all_metrics
)

SELECT 
      sam_number, 
      name,
      num_vehicles,
      num_scheduled_reports,
      num_routes,
      num_addresses,
      num_alerts,
      num_roles,
      safety_score_configured,
      num_api_calls,
      num_route_loads,
      num_handled_events,
      num_events,
      num_tags,
      num_admin_app_loads,
      num_total_vehicles, 
      vg_installation_rate,
      segment,
      industry,
      csm_name,
      ae_name,
      lifetime_acv,
      csm_tier,
      owns_vg,
      owns_ag,
      owns_cm,
      100 * (customer_sophistication_raw_score / (
        SELECT MAX(customer_sophistication_raw_score) 
        FROM customer_sophistication_raw_scores) 
        ) AS customer_sophistication_score
FROM customer_sophistication_raw_scores
