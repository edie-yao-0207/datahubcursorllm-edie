# Cost Attribution

## 📊 Purpose
Break down FirmwareVDP costs by product area, feature, and individual tables to identify optimization opportunities.

## 🎯 Key Dimensions

### Product Group
High-level product categories (e.g., AI & Data, Telematics Platform)

### Dataplatform Feature
ETL/orchestration tools:
- `dagster` - Dagster-orchestrated jobs
- `emr` - EMR-based processing
- `glue` - AWS Glue jobs
- `athena` - Athena queries

### Orchestrator
Execution environment (parsed from service name)

### Table
Individual table costs

## 📈 Analysis Patterns

### Cost Concentration
**Healthy:** Top 5 tables < 50% of total cost
**Warning:** Top 3 tables > 60% of total cost
**Risk:** Single table > 30% of total cost

**Why it matters:** High concentration = single point of failure/cost risk

### Cost by Feature
Compare orchestration costs:
- Dagster jobs typically most expensive (full data scans)
- EMR batch jobs - medium cost
- Glue/Athena - lower cost for simple queries

### Product Group Trends
Track which product areas are growing fastest in cost.

## 💡 Investigation Workflow

1. **Identify Top Cost Driver** (top_tables_by_cost.sql)
2. **Check if Justified** → Join with usage metrics
   - High cost + High usage = Good value
   - High cost + Low usage = Optimization opportunity
3. **Review Feature Mix** (cost_by_product_feature.sql)
   - Is expensive feature necessary?
   - Can we move to cheaper orchestration?
4. **Team Review** (cost_by_team.sql within FirmwareVDP products)

## 🔧 Optimization Strategies

### High-Cost Tables
1. **Partition pruning** - Reduce date ranges
2. **Incremental processing** - Only process new data
3. **Cluster rightsizing** - Match cluster to workload
4. **Caching** - Reuse intermediate results
5. **Sampling** - Use samples for development/testing

### By Feature Type
- **Dagster jobs** → Optimize SQL, reduce full scans
- **EMR jobs** → Review instance types, spot vs on-demand
- **Athena** → Optimize partitioning, use ORC/Parquet

## 🔗 Related Dashboards

- Efficiency → Cost per user/query analysis
- Anomalies → Sudden cost changes
- Usage → Justify costs with adoption metrics

