# Device Safety Settings Table Comparison Analysis

**Date Analyzed:** 2026-02-04  
**Analysis Date:** 2026-02-06  

## Overview

This analysis compares device-level enablement for **Mobile Usage Detection (MUD)** and **Seatbelt** features between two tables:

1. **`product_analytics.dim_devices_safety_settings`** (PA) - Device level safety settings with CM/VG devices paired in a single row
2. **`dojo.device_ai_features_daily_snapshot`** (Dojo) - Daily snapshot of AI features enabled per device

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
| **PA Total (enabled)** | 1,124,996 | - |
| **Dojo Total (enabled)** | 987,759 | - |
| **Overlap (both tables)** | 901,625 | 80.1% of PA, 91.3% of Dojo |
| **PA Only** | 223,371 | 19.9% of PA |
| **Dojo Only** | 86,134 | 8.7% of Dojo |

### Seatbelt

| Metric | Count | Percentage |
|--------|------:|------------|
| **PA Total (enabled)** | 1,061,264 | - |
| **Dojo Total (enabled)** | 911,217 | - |
| **Overlap (both tables)** | 836,584 | 78.8% of PA, 91.8% of Dojo |
| **PA Only** | 224,680 | 21.2% of PA |
| **Dojo Only** | 74,633 | 8.2% of Dojo |

---

## Conclusions

### Key Findings

1. **PA shows more devices enabled than Dojo for both features:**
   - Mobile Usage: PA has 137,237 more devices (~13.9% more)
   - Seatbelt: PA has 150,047 more devices (~16.5% more)

2. **High overlap rate from Dojo's perspective:**
   - ~91% of devices in Dojo are also in PA for both features
   - This suggests Dojo is largely a subset of PA

3. **Significant PA-only devices:**
   - ~20% of PA devices for Mobile Usage don't appear in Dojo
   - ~21% of PA devices for Seatbelt don't appear in Dojo
   - These may represent devices where settings are configured but feature is not actively running

4. **Dojo-only devices are relatively small but notable:**
   - 86,134 devices for Mobile Usage (8.7% of Dojo)
   - 74,633 devices for Seatbelt (8.2% of Dojo)
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
WITH pa AS (
    SELECT DISTINCT org_id, cm_device_id
    FROM product_analytics.dim_devices_safety_settings
    WHERE date = '2026-02-04'
      AND cm_device_id IS NOT NULL
      AND mobile_usage_enabled = true
),
dojo AS (
    SELECT DISTINCT org_id, cm_device_id
    FROM dojo.device_ai_features_daily_snapshot
    WHERE date = '2026-02-04'
      AND feature_enabled = 'haPhonePolicy'
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
WITH pa AS (
    SELECT DISTINCT org_id, cm_device_id
    FROM product_analytics.dim_devices_safety_settings
    WHERE date = '2026-02-04'
      AND cm_device_id IS NOT NULL
      AND seatbelt_enabled = true
),
dojo AS (
    SELECT DISTINCT org_id, cm_device_id
    FROM dojo.device_ai_features_daily_snapshot
    WHERE date = '2026-02-04'
      AND feature_enabled = 'haSeatbeltPolicy'
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
WITH pa AS (
    SELECT DISTINCT org_id, vg_device_id, cm_device_id
    FROM product_analytics.dim_devices_safety_settings
    WHERE date = '2026-02-04'
      AND cm_device_id IS NOT NULL
      AND mobile_usage_enabled = true
),
dojo AS (
    SELECT DISTINCT org_id, vg_device_id, cm_device_id
    FROM dojo.device_ai_features_daily_snapshot
    WHERE date = '2026-02-04'
      AND feature_enabled = 'haPhonePolicy'
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
| 9000765 | 281474981855018 | 281474981854542 |
| 39020 | 281474985272841 | 281474985272840 |
| 2000013 | 154924973784484 | 154924973784677 |
| 4004077 | 281474988095628 | 278018086140199 |
| 8003696 | 281474995251244 | 278018089735399 |
| 11003203 | 281474984448059 | 281474984447565 |
| 9003436 | 281474988954030 | 278018086715657 |
| 7034 | 212014918507903 | 212014918581896 |
| 55630 | 281474977731929 | 278018083027160 |
| 14083 | 281474977089292 | 212014918590545 |
| 7004969 | 281474988196366 | 281474988196128 |
| 7004969 | 281474988203224 | 281474988202981 |
| 10000476 | 281474989660580 | 278018087186817 |
| 67971 | 281474977413991 | 278018085423743 |
| 30271 | 281474977372446 | 212014918927756 |
| 28592 | 212014918904591 | 212014918938979 |
| 7544 | 281474983101506 | 281474977548767 |
| 9006937 | 281474995773321 | 278018090791121 |
| 8725 | 281474997097511 | 278018086723717 |
| 9003099 | 281474982737354 | 281474994547322 |

### Mobile Usage - Dojo Only (enabled in Dojo, not in PA)

| org_id | vg_device_id | cm_device_id |
|--------|--------------|--------------|
| 48578 | 212014918026552 | 278018081748403 |
| 60274 | 281474991705945 | 278018088872584 |
| 60274 | 281474990985779 | 278018088658251 |
| 8003225 | 281474992426228 | 281474992426201 |
| 57668 | 281474978531407 | 281474996671700 |
| 10002039 | 281474981194602 | 278018084590198 |
| 11006083 | 281474989740381 | 281474989740387 |
| 76962 | 281474990575453 | 278018088483842 |
| 4005745 | 281474990518466 | 281474990518467 |
| 10001254 | 281474993016269 | 281474993016266 |
| 60274 | 281474991706404 | 278018088654305 |
| 4001731 | 281474991798168 | 281474980453649 |
| 6006266 | 281474991203041 | 278018088655904 |
| 5009829 | 281475000181671 | 281475000181690 |
| 7006945 | 281474992283937 | 281474992283925 |
| 6005804 | 281474996420272 | 278018090455344 |
| 9004428 | 281474986446926 | 281474986446931 |
| 4004273 | 281474985978618 | 281474985978612 |
| 7003845 | 281474984442592 | 278018086212296 |
| 6000191 | 281474978482031 | 281474978482011 |

### Seatbelt - PA Only (enabled in PA, not in Dojo)

| org_id | vg_device_id | cm_device_id |
|--------|--------------|--------------|
| 9000765 | 281474981855018 | 281474981854542 |
| 39020 | 281474985272841 | 281474985272840 |
| 2000013 | 154924973784484 | 154924973784677 |
| 58743 | 281474977943719 | 278018082522925 |
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
| 8001256 | 281474979160239 | 281474979160257 |
| 28592 | 212014918904591 | 212014918938979 |

### Seatbelt - Dojo Only (enabled in Dojo, not in PA)

| org_id | vg_device_id | cm_device_id |
|--------|--------------|--------------|
| 10002485 | 281474983078836 | 278018092177253 |
| 8005879 | 281474989581919 | 278018087165003 |
| 53576 | 278018081719424 | 278018081825664 |
| 3408 | 281474991085189 | 278018081937701 |
| 867 | 281474991676012 | 278018088507513 |
| 867 | 281474978984308 | 278018089633151 |
| 867 | 281474978429933 | 278018083023492 |
| 8009783 | 281474999880238 | 281474999880240 |
| 79181 | 281474978143147 | 281474978143130 |
| 43287 | 281474999143075 | 281474999143076 |
| 66068 | 281474977387855 | 281474977387860 |
| 6004679 | 281474993450165 | 281474993450163 |
| 21306 | 281474977475186 | 281474977475167 |
| 52705 | 281474986816234 | 278018081886214 |
| 7002964 | 281474982439332 | 278018084886832 |
| 867 | 281474996046919 | 278018085488890 |
| 53900 | 281474993653943 | 281474983594861 |
| 9000305 | 281474979497761 | 281474979497762 |
| 6001679 | 281474999938154 | 278018093363831 |
| 6005804 | 281474995450577 | 281474995450584 |

---

## Next Steps for Investigation

1. **Verify sample devices on Cloud Dashboard** - Check the listed devices to determine which table reflects the true enablement state
2. **Investigate PA-only devices** - Determine if these are configured but not actively running
3. **Investigate Dojo-only devices** - Check if these represent timing issues or missed configurations in PA
4. **Review data pipeline timing** - Compare when each table is refreshed to understand potential lag
