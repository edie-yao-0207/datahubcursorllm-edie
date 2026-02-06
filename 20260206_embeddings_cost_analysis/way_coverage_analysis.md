# Way ID Coverage Analysis for Embeddings Cost Optimization

## Overview

This analysis identifies the minimum set of organizations whose CM33 fleet covers all observed way_ids (road segments) with minimum total trip seconds. The goal is to optimize embeddings/data collection costs by selecting orgs that provide maximum road coverage with minimum driving time.

## Methodology

### Data Sources
- `kinesisstats.location` - Location data with way_id (OpenStreetMap road segment identifiers)
- `datamodel_core.dim_devices` - Device information including CM33 camera product
- `datamodel_core.dim_organizations` - Organization metadata including `is_paid_safety_customer`
- `datamodel_core.lifetime_device_activity` - Device activity status (l7 > 0 = active in last 7 days)
- `datamodel_telematics.fct_trips` - Trip data for calculating trip seconds
- `dojo.ml_blocklisted_org_ids` - Blocklisted organizations to exclude

### Filters Applied
| Filter | Criteria |
|--------|----------|
| Device type | CM33 only (camera_product_id = 167) |
| Org type | Customer orgs only (internal_type = 0) |
| Safety customer | is_paid_safety_customer = true |
| Device activity | l7 > 0 (active 1+ day in last 7 days) |
| Min devices per org | 10+ active CM33 devices |
| Blocklist | Excluded orgs in dojo.ml_blocklisted_org_ids |

### Sampling Strategy
- **Date range**: Jan 31 - Feb 5, 2026 (6 days)
- **Time sampling**: First 10 seconds of each hour (minute = 0, second < 10)
- **Join date**: Feb 5, 2026 (last day of week) for device/org filters

### Algorithm
Weighted Greedy Set Cover:
1. For each org, collect the set of unique way_ids their CM33 devices travel
2. For each org, calculate total trip seconds for the week
3. Iteratively select the org with best cost-effectiveness ratio: `trip_seconds / new_ways_covered`
4. Continue until all way_ids are covered

## Results

### Qualified Organizations
| Metric | Value |
|--------|-------|
| Qualified orgs | 3,928 |
| Total CM33 devices | 138,115 |

### Way ID Coverage
| Metric | Value |
|--------|-------|
| Total unique way_ids | 1,005,283 |
| Way_ids per org (avg) | ~256 |

### Optimization Outcome
| Metric | Value |
|--------|-------|
| **Orgs needed for 100% coverage** | **3,928** (all qualified orgs) |
| **Total trip seconds** | 9,978,770,407 |
| **Total trip hours** | ~2,771,881 hours |

## Key Findings

1. **Full coverage requires all orgs**: Unlike grid-based analysis where a subset of orgs can cover all geographic grids, way_id coverage requires all 3,928 qualified orgs to achieve 100% coverage.

2. **Highly distributed coverage**: Each organization covers some unique way_ids (road segments) that no other organization covers. This reflects the granular nature of way_ids - each represents a specific road segment (typically 50-500 meters), not a broad geographic area.

3. **Coverage progression**:
   - 50 orgs → 1.1% coverage
   - 500 orgs → 15.6% coverage
   - 1,000 orgs → 31.5% coverage
   - 2,000 orgs → 58.9% coverage
   - 3,000 orgs → 84.8% coverage
   - 3,500 orgs → 95.4% coverage
   - 3,928 orgs → 100% coverage

4. **Implication for cost optimization**: If partial coverage is acceptable, significant cost reduction is possible:
   - 50% coverage with ~1,650 orgs
   - 80% coverage with ~2,700 orgs
   - 95% coverage with ~3,500 orgs

## Comparison: Way ID vs Grid Coverage

| Approach | Coverage Unit | # Unique Units | Orgs for 100% |
|----------|---------------|----------------|---------------|
| Grid (80km) | ~80x80 km cells | ~1,500-2,000 | ~500-700 |
| Way ID | Road segments | 1,005,283 | 3,928 |

Way_id coverage is ~500x more granular than grid coverage, which explains why more orgs are needed.

## Files

- `way_coverage_analysis.md` - This analysis document
- `run_way_coverage_min_orgs.py` - Python script to run the analysis

## Usage

```bash
# Set up environment
source .envrc  # or: direnv allow

# Run analysis
python run_way_coverage_min_orgs.py
```

## Date

Analysis run: February 6, 2026
