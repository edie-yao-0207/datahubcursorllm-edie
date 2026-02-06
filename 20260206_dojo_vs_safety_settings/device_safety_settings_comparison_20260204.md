# Device Safety Settings Table Comparison Analysis

**Date Analyzed:** 2026-02-04  
**Analysis Date:** 2026-02-06  

## Overview

This analysis compares device-level enablement for **Mobile Usage Detection (MUD)** and **Seatbelt** features between two tables:

1. **`product_analytics.dim_devices_safety_settings`** (PA) - Device level safety settings with CM/VG devices paired in a single row
2. **`dojo.device_ai_features_daily_snapshot`** (Dojo) - Daily snapshot of AI features enabled per device

### Filters Applied
- **Paid Customer Orgs Only**: Both tables joined with `datamodel_core.dim_organizations` filtered to `is_paid_customer = true`
- **Active Devices Only**: Both tables joined with `datamodel_core.lifetime_device_activity` filtered to `l1 > 0` (active on that day)
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
| **PA Total (enabled)** | 652,738 | - |
| **Dojo Total (enabled)** | 703,815 | - |
| **Overlap (both tables)** | 646,628 | 99.1% of PA, 91.9% of Dojo |
| **PA Only** | 6,110 | 0.9% of PA |
| **Dojo Only** | 57,187 | 8.1% of Dojo |

### Seatbelt

| Metric | Count | Percentage |
|--------|------:|------------|
| **PA Total (enabled)** | 607,523 | - |
| **Dojo Total (enabled)** | 650,418 | - |
| **Overlap (both tables)** | 599,524 | 98.7% of PA, 92.2% of Dojo |
| **PA Only** | 7,999 | 1.3% of PA |
| **Dojo Only** | 50,894 | 7.8% of Dojo |

---

## Conclusions

### Key Findings

1. **Dojo shows more devices enabled than PA for both features (when filtering to active devices):**
   - Mobile Usage: Dojo has 51,077 more devices (~7.8% more)
   - Seatbelt: Dojo has 42,895 more devices (~7.1% more)

2. **Very high overlap rate from PA's perspective:**
   - ~99% of devices in PA are also in Dojo for both features
   - PA is almost entirely a subset of Dojo when filtering to active devices

3. **Small PA-only devices:**
   - Only ~1% of PA devices for Mobile Usage don't appear in Dojo (6,110 devices)
   - Only ~1.3% of PA devices for Seatbelt don't appear in Dojo (7,999 devices)

4. **Notable Dojo-only devices:**
   - 57,187 devices for Mobile Usage (8.1% of Dojo)
   - 50,894 devices for Seatbelt (7.8% of Dojo)
   - These represent devices where the feature is actively running but not reflected in PA settings

### Key Insight: Active Device Filter Impact

With the active devices filter applied:
- **Dojo now has MORE devices than PA** (opposite of without the filter)
- PA's "extra" devices were largely inactive devices with settings configured
- Dojo more accurately reflects which devices are actively using the features

### Possible Explanations for Discrepancies

1. **Dojo-only devices**: Feature may be running even though PA settings table doesn't show it enabled
2. **PA-only devices**: Settings configured but feature not actually running (small ~1% discrepancy)
3. **Timing differences**: Data pipeline refresh timing between the two tables

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
active_devices AS (
    SELECT DISTINCT org_id, device_id
    FROM datamodel_core.lifetime_device_activity
    WHERE date = '2026-02-04'
      AND l1 > 0
),
pa AS (
    SELECT DISTINCT p.org_id, p.cm_device_id
    FROM product_analytics.dim_devices_safety_settings p
    INNER JOIN paid_orgs o ON p.org_id = o.org_id
    INNER JOIN active_devices a ON p.org_id = a.org_id AND p.cm_device_id = a.device_id
    WHERE p.date = '2026-02-04'
      AND p.cm_device_id IS NOT NULL
      AND p.mobile_usage_enabled = true
),
dojo AS (
    SELECT DISTINCT d.org_id, d.cm_device_id
    FROM dojo.device_ai_features_daily_snapshot d
    INNER JOIN paid_orgs o ON d.org_id = o.org_id
    INNER JOIN active_devices a ON d.org_id = a.org_id AND d.cm_device_id = a.device_id
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
active_devices AS (
    SELECT DISTINCT org_id, device_id
    FROM datamodel_core.lifetime_device_activity
    WHERE date = '2026-02-04'
      AND l1 > 0
),
pa AS (
    SELECT DISTINCT p.org_id, p.cm_device_id
    FROM product_analytics.dim_devices_safety_settings p
    INNER JOIN paid_orgs o ON p.org_id = o.org_id
    INNER JOIN active_devices a ON p.org_id = a.org_id AND p.cm_device_id = a.device_id
    WHERE p.date = '2026-02-04'
      AND p.cm_device_id IS NOT NULL
      AND p.seatbelt_enabled = true
),
dojo AS (
    SELECT DISTINCT d.org_id, d.cm_device_id
    FROM dojo.device_ai_features_daily_snapshot d
    INNER JOIN paid_orgs o ON d.org_id = o.org_id
    INNER JOIN active_devices a ON d.org_id = a.org_id AND d.cm_device_id = a.device_id
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
active_devices AS (
    SELECT DISTINCT org_id, device_id
    FROM datamodel_core.lifetime_device_activity
    WHERE date = '2026-02-04'
      AND l1 > 0
),
pa AS (
    SELECT DISTINCT p.org_id, p.vg_device_id, p.cm_device_id
    FROM product_analytics.dim_devices_safety_settings p
    INNER JOIN paid_orgs o ON p.org_id = o.org_id
    INNER JOIN active_devices a ON p.org_id = a.org_id AND p.cm_device_id = a.device_id
    WHERE p.date = '2026-02-04'
      AND p.cm_device_id IS NOT NULL
      AND p.mobile_usage_enabled = true
),
dojo AS (
    SELECT DISTINCT d.org_id, d.vg_device_id, d.cm_device_id
    FROM dojo.device_ai_features_daily_snapshot d
    INNER JOIN paid_orgs o ON d.org_id = o.org_id
    INNER JOIN active_devices a ON d.org_id = a.org_id AND d.cm_device_id = a.device_id
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
| 11005223 | 281474990523522 | 278018093023109 |
| 26786 | 281474997287635 | 281474994444486 |
| 70958 | 281475000282549 | 278018092276143 |
| 6953 | 212014919053034 | 278018081757796 |
| 5005444 | 281474996697198 | 281474996697169 |
| 11005223 | 281474989590315 | 278018093010259 |
| 8005276 | 281474988179882 | 278018087187374 |
| 11007831 | 281474995957040 | 278018090461550 |
| 6007789 | 281474995417135 | 281474995417106 |
| 17667 | 281474984127855 | 278018085069887 |
| 10007546 | 281475000705460 | 278018093168775 |
| 83330 | 281474984494479 | 278018085062225 |
| 6007465 | 281474995499369 | 278018091478341 |
| 7004969 | 281474994913067 | 281474988211145 |
| 6007789 | 281474999770799 | 278018093151694 |
| 7002031 | 281474993416691 | 278018084332730 |
| 11002502 | 281474986607643 | 278018085488530 |
| 32848 | 212014918737153 | 278018092819741 |
| 83330 | 281474983056298 | 278018084548756 |
| 23308 | 281474990776884 | 278018086211067 |

### Mobile Usage - Dojo Only (enabled in Dojo, not in PA)

| org_id | vg_device_id | cm_device_id |
|--------|--------------|--------------|
| 11009572 | 281475000159361 | 281475000159416 |
| 6009550 | 281475000012654 | 281475000012658 |
| 9000764 | 281474999330283 | 281474982321598 |
| 60274 | 281474991096829 | 278018088633075 |
| 66503 | 281474990096257 | 278018088361805 |
| 5002621 | 281474990508465 | 281474990509113 |
| 44852 | 281474990617399 | 281474989805509 |
| 46607 | 212014918994244 | 212014918864564 |
| 71864 | 281474978296474 | 278018082880638 |
| 10005195 | 281474991676259 | 281474991676211 |
| 10005799 | 281474993763945 | 278018088485465 |
| 11000504 | 281474979567129 | 281474979567128 |
| 5000706 | 281474981316191 | 278018084287357 |
| 6007426 | 281474994446818 | 281474994446862 |
| 18222 | 281474995815065 | 278018089645523 |
| 70280 | 281475000333563 | 278018084329465 |
| 8559 | 281474995213640 | 278018085430939 |
| 6008108 | 281474996381086 | 281474996381123 |
| 7002467 | 281474981648071 | 281474981648075 |
| 66111 | 281474979250672 | 281474977365366 |

### Seatbelt - PA Only (enabled in PA, not in Dojo)

| org_id | vg_device_id | cm_device_id |
|--------|--------------|--------------|
| 11008251 | 281475000488965 | 278018085479943 |
| 11006027 | 281474996272519 | 278018089637921 |
| 9389 | 281474996341664 | 278018089634097 |
| 11006027 | 281474999423827 | 278018092450190 |
| 9389 | 281474999329362 | 278018092280250 |
| 11006090 | 281474992936593 | 278018088901572 |
| 10008304 | 281474999626984 | 278018091208447 |
| 11008251 | 281475000489012 | 278018088910137 |
| 11008934 | 281474998280170 | 281474998280160 |
| 6007156 | 281475000698026 | 278018093940028 |
| 4007808 | 281474998941362 | 278018091227476 |
| 11006027 | 281474995794258 | 278018089631694 |
| 7009288 | 281474998906213 | 281474998906211 |
| 76772 | 281474996608107 | 278018086217773 |
| 11005223 | 281474989580102 | 278018093313404 |
| 8003107 | 281474990183253 | 281474988565310 |
| 11006027 | 281474995491143 | 278018089620830 |
| 7009504 | 281474999664571 | 281474999664590 |
| 6002337 | 281474999051956 | 278018089534538 |
| 4001913 | 281474984199857 | 278018083879931 |

### Seatbelt - Dojo Only (enabled in Dojo, not in PA)

| org_id | vg_device_id | cm_device_id |
|--------|--------------|--------------|
| 5004953 | 281474988463124 | 278018086165884 |
| 7003474 | 281474983615788 | 281474983616109 |
| 8004699 | 281474991016491 | 278018086706421 |
| 10001737 | 281474989013889 | 278018086726251 |
| 11006291 | 281474990837223 | 278018090691499 |
| 867 | 281474990884653 | 278018092826135 |
| 5001055 | 281474993962025 | 278018088879249 |
| 6004766 | 281474999934482 | 278018089952059 |
| 57255 | 281474993713931 | 281474978016243 |
| 9007218 | 281474995993605 | 281474995993604 |
| 5005891 | 281474994320070 | 278018090001124 |
| 41783 | 281474992122866 | 212014918928502 |
| 9003638 | 281474985119386 | 281474985119385 |
| 60478 | 281474978520928 | 278018088531102 |
| 5001055 | 281474997158641 | 281474994569517 |
| 17644 | 281474992628594 | 278018088648722 |
| 7005422 | 281474989981965 | 278018088376929 |
| 17644 | 281474993526831 | 281474986781539 |
| 9002761 | 281474995689643 | 281474981939627 |
| 10005053 | 281474987968748 | 281474987968672 |

---

## Next Steps for Investigation

1. **Verify sample devices on Cloud Dashboard** - Check the listed devices to determine which table reflects the true enablement state
2. **Investigate PA-only devices** - Settings configured but not appearing in Dojo (very small ~1% discrepancy)
3. **Investigate Dojo-only devices** - Feature running but not reflected in PA settings (~8% of Dojo)
4. **Review data pipeline timing** - Compare when each table is refreshed to understand potential lag
