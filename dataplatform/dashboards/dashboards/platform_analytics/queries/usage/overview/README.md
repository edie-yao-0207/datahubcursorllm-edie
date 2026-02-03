# Usage Overview

## 📊 Purpose
High-level snapshot of data platform adoption and engagement for FirmwareVDP-owned tables.

## 📁 Queries

| Query | Visualization | Purpose |
|-------|---------------|---------|
| `usage_snapshot.sql` | Scorecards | At-a-glance metrics: MAU, queries, tables, growth |
| `usage_summary.sql` | Line Charts | DAU/WAU/MAU and query volume over time |
| `usage_growth.sql` | Line Charts | Growth rate trends |

## 🎯 Key Metrics

### Monthly Active Users (MAU)
**Definition:** Unique users who queried any FirmwareVDP table in the last 30 days

**Healthy Trends:**
- 🟢 Steady growth (5-10% per month)
- 🟡 Flat (stable user base)
- 🔴 Declining (users churning or migrating)

### Weekly Active Users (WAU)
**Definition:** Unique users in last 7 days

### Daily Active Users (DAU)
**Definition:** Unique users today

### DAU/WAU Ratio
**Formula:** Daily users ÷ Weekly users

**Target:** > 0.6 (high engagement)

**What it means:**
- How frequently weekly users return
- 1.0 = all weekly users are active daily
- 0.3 = users only check in 2-3 times per week

### Query Volume
Total queries executed against FirmwareVDP tables.

**What to watch:**
- Sudden spikes (runaway queries or legitimate feature launch)
- Declining volume despite stable users (users finding data elsewhere)

### Unique Tables Accessed
How many distinct tables are being used.

**Health Check:**
- High number = good data discovery
- Low number = users concentrated on few tables
- Declining = tables becoming stale/unused

## 📈 Growth Metrics

### User Growth Rate
**Formula:** (Current MAU - Previous MAU) ÷ Previous MAU

**Targets:**
- Early stage: 10-20% monthly growth
- Mature: 3-5% monthly growth
- Steady state: 0-2% growth

### Query Growth Rate
Similar to user growth but for query volume.

**Watch for:** Query growth >> User growth (potential runaway queries)

## 💡 How to Interpret

### Healthy Platform
✅ Steady MAU growth  
✅ DAU/WAU > 0.6 (high engagement)  
✅ Query volume grows proportionally with users  
✅ Increasing unique tables accessed (discovery)

### Warning Signs
🟡 MAU flat for >2 months  
🟡 DAU/WAU < 0.4 (low engagement)  
🟡 Query volume declining  
🟡 Unique tables accessed shrinking

### Critical Issues
🔴 MAU declining for >1 month (churn)  
🔴 Query volume spike without user growth (runaway)  
🔴 <5 unique tables accessed (limited utility)

## 🔗 Drill-Down

- **More user details** → See `usage/engagement/` and `usage/growth/` tabs
- **Data freshness** → See `reliability/` tab
- **Cost implications** → See `cost/` tab
