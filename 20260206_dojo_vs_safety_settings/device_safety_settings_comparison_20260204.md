# Device Safety Settings Table Comparison Analysis

**Date Analyzed:** 2026-02-04  
**Analysis Date:** 2026-02-06  

## Overview

This analysis compares device-level enablement for **Mobile Usage Detection (MUD)** and **Seatbelt** features between two tables:

1. **`product_analytics.dim_devices_safety_settings`** (PA) - Device level safety settings with CM/VG devices paired in a single row
2. **`dojo.device_ai_features_daily_snapshot`** (Dojo) - Daily snapshot of AI features enabled per device

### Filters Applied
- **Paid Customer Orgs Only**: Both tables joined with `datamodel_core.dim_organizations` filtered to `is_paid_customer = true`
- **CM Devices Only**: PA table filtered to `cm_device_id IS NOT NULL`

### Column Mappings

| Feature | PA Column | Dojo Column |
|---------|-----------|-------------|
| Mobile Usage Detection | `mobile_usage_enabled = true` | `feature_enabled = 'haPhonePolicy'` |
| Seatbelt | `seatbelt_enabled = true` | `feature_enabled = 'haSeatbeltPolicy'` |

**Note:** In Dojo, if a feature is enabled on a given day, it appears as a row with `feature_enabled` set to the corresponding policy name.

---

## Summary Comparison

### Mobile Usage Detection (MUD)

| Metric | Count | Percentage |
|--------|------:|------------|
| **PA Total (enabled)** | 1,050,857 | - |
| **Dojo Total (enabled)** | 965,348 | - |
| **Overlap (both tables)** | 883,429 | 84.1% of PA, 91.5% of Dojo |
| **PA Only** | 167,428 | 15.9% of PA |
| **Dojo Only** | 81,919 | 8.5% of Dojo |

### Seatbelt

| Metric | Count | Percentage |
|--------|------:|------------|
| **PA Total (enabled)** | 984,590 | - |
| **Dojo Total (enabled)** | 891,958 | - |
| **Overlap (both tables)** | 820,525 | 83.3% of PA, 92.0% of Dojo |
| **PA Only** | 164,065 | 16.7% of PA |
| **Dojo Only** | 71,433 | 8.0% of Dojo |

---

## Conclusions

### Key Findings

1. **PA shows more devices enabled than Dojo for both features:**
   - Mobile Usage: PA has 85,509 more devices (~8.9% more)
   - Seatbelt: PA has 92,632 more devices (~10.4% more)

2. **High overlap rate from Dojo's perspective:**
   - ~91-92% of devices in Dojo are also in PA for both features
   - This suggests Dojo is largely a subset of PA

3. **Significant PA-only devices:**
   - ~16% of PA devices for Mobile Usage don't appear in Dojo
   - ~17% of PA devices for Seatbelt don't appear in Dojo
   - These may represent devices where settings are configured but feature is not actively running

4. **Dojo-only devices are relatively small but notable:**
   - 81,919 devices for Mobile Usage (8.5% of Dojo)
   - 71,433 devices for Seatbelt (8.0% of Dojo)
   - These may represent recent enablements not yet reflected in PA, or data synchronization gaps

### Possible Explanations for Discrepancies

1. **Data freshness/timing differences** between the two pipelines
2. **Definition differences** - PA may include configured settings while Dojo tracks active feature runs
3. **Device pairing logic** - Different approaches to matching VG/CM device pairs
4. **Filter criteria** - PA query filters on `cm_device_id IS NOT NULL` which may exclude some devices

---

## SQL Queries Used

### Mobile Usage Detection Comparison

```sql
WITH paid_orgs AS (
    SELECT DISTINCT org_id
    FROM datamodel_core.dim_organizations
    WHERE date = '2026-02-04'
      AND is_paid_customer = true
),
pa AS (
    SELECT DISTINCT p.org_id, p.cm_device_id
    FROM product_analytics.dim_devices_safety_settings p
    INNER JOIN paid_orgs o ON p.org_id = o.org_id
    WHERE p.date = '2026-02-04'
      AND p.cm_device_id IS NOT NULL
      AND p.mobile_usage_enabled = true
),
dojo AS (
    SELECT DISTINCT d.org_id, d.cm_device_id
    FROM dojo.device_ai_features_daily_snapshot d
    INNER JOIN paid_orgs o ON d.org_id = o.org_id
    WHERE d.date = '2026-02-04'
      AND d.feature_enabled = 'haPhonePolicy'
),
counts AS (
    SELECT 
        (SELECT COUNT(*) FROM pa) AS pa_count,
        (SELECT COUNT(*) FROM dojo) AS dojo_count,
        (SELECT COUNT(*) FROM pa INNER JOIN dojo ON pa.org_id = dojo.org_id AND pa.cm_device_id = dojo.cm_device_id) AS overlap,
        (SELECT COUNT(*) FROM pa LEFT JOIN dojo ON pa.org_id = dojo.org_id AND pa.cm_device_id = dojo.cm_device_id WHERE dojo.cm_device_id IS NULL) AS pa_only,
        (SELECT COUNT(*) FROM pa RIGHT JOIN dojo ON pa.org_id = dojo.org_id AND pa.cm_device_id = dojo.cm_device_id WHERE pa.cm_device_id IS NULL) AS dojo_only
)
SELECT * FROM counts
```

### Seatbelt Comparison

```sql
WITH paid_orgs AS (
    SELECT DISTINCT org_id
    FROM datamodel_core.dim_organizations
    WHERE date = '2026-02-04'
      AND is_paid_customer = true
),
pa AS (
    SELECT DISTINCT p.org_id, p.cm_device_id
    FROM product_analytics.dim_devices_safety_settings p
    INNER JOIN paid_orgs o ON p.org_id = o.org_id
    WHERE p.date = '2026-02-04'
      AND p.cm_device_id IS NOT NULL
      AND p.seatbelt_enabled = true
),
dojo AS (
    SELECT DISTINCT d.org_id, d.cm_device_id
    FROM dojo.device_ai_features_daily_snapshot d
    INNER JOIN paid_orgs o ON d.org_id = o.org_id
    WHERE d.date = '2026-02-04'
      AND d.feature_enabled = 'haSeatbeltPolicy'
),
counts AS (
    SELECT 
        (SELECT COUNT(*) FROM pa) AS pa_count,
        (SELECT COUNT(*) FROM dojo) AS dojo_count,
        (SELECT COUNT(*) FROM pa INNER JOIN dojo ON pa.org_id = dojo.org_id AND pa.cm_device_id = dojo.cm_device_id) AS overlap,
        (SELECT COUNT(*) FROM pa LEFT JOIN dojo ON pa.org_id = dojo.org_id AND pa.cm_device_id = dojo.cm_device_id WHERE dojo.cm_device_id IS NULL) AS pa_only,
        (SELECT COUNT(*) FROM pa RIGHT JOIN dojo ON pa.org_id = dojo.org_id AND pa.cm_device_id = dojo.cm_device_id WHERE pa.cm_device_id IS NULL) AS dojo_only
)
SELECT * FROM counts
```

### Sample Devices Query (PA Only - Mobile Usage)

```sql
WITH paid_orgs AS (
    SELECT DISTINCT org_id
    FROM datamodel_core.dim_organizations
    WHERE date = '2026-02-04'
      AND is_paid_customer = true
),
pa AS (
    SELECT DISTINCT p.org_id, p.vg_device_id, p.cm_device_id
    FROM product_analytics.dim_devices_safety_settings p
    INNER JOIN paid_orgs o ON p.org_id = o.org_id
    WHERE p.date = '2026-02-04'
      AND p.cm_device_id IS NOT NULL
      AND p.mobile_usage_enabled = true
),
dojo AS (
    SELECT DISTINCT d.org_id, d.vg_device_id, d.cm_device_id
    FROM dojo.device_ai_features_daily_snapshot d
    INNER JOIN paid_orgs o ON d.org_id = o.org_id
    WHERE d.date = '2026-02-04'
      AND d.feature_enabled = 'haPhonePolicy'
)
SELECT 
    'pa_only_mobile_usage' AS source,
    CAST(pa.org_id AS STRING) AS org_id,
    CAST(pa.vg_device_id AS STRING) AS vg_device_id,
    CAST(pa.cm_device_id AS STRING) AS cm_device_id
FROM pa
LEFT JOIN dojo ON pa.org_id = dojo.org_id AND pa.cm_device_id = dojo.cm_device_id
WHERE dojo.cm_device_id IS NULL
LIMIT 20
```

---

## Sample Devices for Cloud Dashboard Verification

### Mobile Usage - PA Only (enabled in PA, not in Dojo)

| org_id | vg_device_id | cm_device_id |
|--------|--------------|--------------|
| 59432 | 281474985381149 | 281474977186813 |
| 72989 | 281474995266847 | 278018089625151 |
| 78892 | 281474978224804 | 278018082850042 |
| 4002469 | 281474981114424 | 281474981114448 |
| 6005622 | 281474988620041 | 278018083615417 |
| 7003925 | 281474984616257 | 281474984616286 |
| 38765 | 281474981572891 | 278018084291477 |
| 45402 | 281474989825954 | 278018086202169 |
| 78843 | 281474987759879 | 278018085806532 |
| 79298 | 281474990359451 | 281474988046745 |
| 4004542 | 281474996024194 | 278018089986200 |
| 8004116 | 281474987906273 | 281474987906349 |
| 8006811 | 281474991715208 | 281474993996472 |
| 10008114 | 281474997447489 | 281474997447203 |
| 8725 | 281474997022274 | 278018085524893 |
| 11006306 | 281474990693182 | 281474990475794 |
| 71862 | 281474977684873 | 281474977685324 |
| 6004170 | 281474988147323 | 278018086139655 |
| 10002405 | 281474987032506 | 281474987032507 |
| 34450 | 212014918030183 | 212014919028330 |

### Mobile Usage - Dojo Only (enabled in Dojo, not in PA)

| org_id | vg_device_id | cm_device_id |
|--------|--------------|--------------|
| 5009816 | 281475000480075 | 281475000480149 |
| 53154 | 281474990271378 | 278018088529903 |
| 7099 | 281474991878203 | 278018084545023 |
| 4003037 | 281474982584374 | 281474982584338 |
| 57623 | 281474988649487 | 281474977327423 |
| 7002935 | 281474994279960 | 278018089999945 |
| 11000450 | 281474995836401 | 278018087166939 |
| 14289 | 281474985464047 | 281474985464110 |
| 9002748 | 281474996394018 | 281474982755882 |
| 57255 | 281474993713931 | 281474978016243 |
| 10005082 | 281474993312776 | 278018089366407 |
| 60274 | 281474991709329 | 278018088920467 |
| 8004742 | 281474986451216 | 281474987371738 |
| 56630 | 281474994871650 | 278018090165768 |
| 60274 | 281474991097661 | 278018088660427 |
| 7002430 | 281474989747763 | 278018087421415 |
| 9003491 | 281474988537069 | 281474988536996 |
| 19568 | 281474986933725 | 281474994457951 |
| 9003828 | 281474985763632 | 278018085134777 |
| 6006533 | 281474994259702 | 281474994259713 |

### Seatbelt - PA Only (enabled in PA, not in Dojo)

| org_id | vg_device_id | cm_device_id |
|--------|--------------|--------------|
| 9000765 | 281474981855018 | 281474981854542 |
| 39020 | 281474985272841 | 281474985272840 |
| 4004077 | 281474988095628 | 278018086140199 |
| 8003696 | 281474995251244 | 278018089735399 |
| 11003203 | 281474984448059 | 281474984447565 |
| 9003436 | 281474988954030 | 278018086715657 |
| 4000796 | 281474986617159 | 278018086127887 |
| 7034 | 212014918507903 | 212014918581896 |
| 15781 | 212014918487327 | 31212014918487327 |
| 11006027 | 281474999423827 | 278018092450190 |
| 14083 | 281474977089292 | 212014918590545 |
| 9389 | 281474996151452 | 278018089731465 |
| 7004969 | 281474988196366 | 281474988196128 |
| 7004969 | 281474988203224 | 281474988202981 |
| 10000476 | 281474989660580 | 278018087186817 |
| 30271 | 281474977372446 | 212014918927756 |
| 28592 | 212014918904591 | 212014918938979 |
| 7544 | 281474983101506 | 281474977548767 |
| 9006937 | 281474995773321 | 278018090791121 |
| 8725 | 281474997097511 | 278018086723717 |

### Seatbelt - Dojo Only (enabled in Dojo, not in PA)

| org_id | vg_device_id | cm_device_id |
|--------|--------------|--------------|
| 7009674 | 281475000210239 | 281475000210255 |
| 8002145 | 281474989830096 | 281474989830081 |
| 80873 | 281474998792911 | 278018091909646 |
| 5005891 | 281474998784203 | 278018088668762 |
| 6003793 | 281474985102566 | 281474984467021 |
| 7005331 | 281474993770776 | 281474993770777 |
| 58878 | 281474981897984 | 278018084588832 |
| 8002216 | None | 281474982239998 |
| 8005884 | 281474989522494 | 281474989522513 |
| 10001064 | 281474999402341 | 278018089366703 |
| 82641 | 281474985496959 | 278018085477337 |
| 5004096 | 281475000099123 | 278018088884909 |
| 7001070 | 281474998603811 | 278018091646663 |
| 20967 | 281474979945018 | 281474979945019 |
| 8004766 | 281474987361517 | 278018090973019 |
| 35575 | 212014918736130 | 212014918864782 |
| 45950 | 281474988799314 | 278018086164803 |
| 45982 | 281474983421505 | 281474983421506 |
| 11000854 | 281474997057420 | 281474997057265 |
| 68129 | 281474991787498 | 281474991787499 |

---

## Next Steps for Investigation

1. **Verify sample devices on Cloud Dashboard** - Check the listed devices to determine which table reflects the true enablement state
2. **Investigate PA-only devices** - Determine if these are configured but not actively running
3. **Investigate Dojo-only devices** - Check if these represent timing issues or missed configurations in PA
4. **Review data pipeline timing** - Compare when each table is refreshed to understand potential lag
