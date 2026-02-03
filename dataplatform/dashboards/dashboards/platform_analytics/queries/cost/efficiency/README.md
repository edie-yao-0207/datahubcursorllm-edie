# Cost Efficiency

## 📊 Purpose
Measure value delivered per dollar spent - are we getting good ROI on our data platform investment?

## 🎯 Key Efficiency Metrics

### Cost per Active User
**Formula:** Monthly cost ÷ Monthly active users

**Target:** < $50 per user per month

**What it means:**
- How much we spend to serve each data consumer
- Tracks platform efficiency as usage scales
- Identifies if cost is growing faster than adoption

**Healthy Trends:**
- 🟢 Declining over time (economies of scale)
- 🟡 Flat (maintaining efficiency as we grow)
- 🔴 Rising (cost outpacing adoption)

### Cost per 1K Queries
**Formula:** Monthly cost ÷ Monthly queries × 1000

**Target:** < $1 per 1000 queries

**What it means:**
- Marginal cost of serving a query
- Lower is better (efficient data access)

**Optimization Levers:**
- Query optimization (reduce scans)
- Caching (avoid recomputation)
- Materialized views (pre-aggregate)

### Idle Table Cost
**Definition:** Tables with zero or very low usage (<3 users/month) but material cost (>$10/month)

**Target:** < 5% of total team cost

**What it means:**
- Wasted spend on unused data
- Deprecation candidates

**Actions:**
1. Contact table owner
2. Verify if still needed
3. Deprecate or reduce refresh frequency
4. Move to on-demand processing

### Cost Efficiency Degradation
**Definition:** Tables where cost is rising but usage is falling

**Red Flags:**
- 🔴 Cost ↑ + Users ↓ = Inefficiency
- 🟡 High cost + Few users = Underutilized

**Common Causes:**
- Data volume growth without corresponding usage
- Jobs running more frequently than needed
- Users migrated to other tables but old ones still running

## 💡 Optimization Playbook

### High Cost per User
1. **Investigate top cost tables** - Are they all necessary?
2. **Reduce frequency** - Do jobs need to run daily?
3. **Share infrastructure** - Combine similar workloads
4. **Cache results** - Avoid recomputation

### Idle Tables
1. **Deprecation** - Remove if unused for 60+ days
2. **Reduce frequency** - Weekly instead of daily
3. **On-demand** - Only run when explicitly triggered
4. **Archive** - Move to cold storage

### Rising Cost per Query
1. **Query optimization** - Add partition filters
2. **Denormalization** - Pre-join frequently used tables
3. **Aggregation tables** - Pre-compute common rollups
4. **Index/stats** - Update table statistics

## 📏 Benchmarking

**Industry Targets:**
- Cost per active user: $20-50/month
- Cost per 1K queries: $0.10-1.00
- Idle table waste: <5% of budget

**FirmwareVDP Targets:**
- Set based on your budget and user base
- Review quarterly and adjust

## 🔗 Related Metrics

- Attribution → Which tables are expensive
- Usage → Are users actively using expensive tables
- Anomalies → Sudden efficiency changes

