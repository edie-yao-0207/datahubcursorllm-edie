# Cost Anomalies

## 📊 Purpose
Detect and investigate unusual cost patterns using statistical anomaly detection (z-scores).

## 🎯 Anomaly Severity Levels

### 🟢 Normal
Z-score < 2σ - Within expected range

### 🟡 Warning
Z-score 2-3σ - Elevated but not critical

### 🟠 Anomalous
Z-score 3-5σ - Significant deviation requiring investigation

### 🔴 Critical
Z-score > 5σ - Extreme deviation requiring immediate action

## 📈 Anomaly Types

### Increases
- **Critical Increase**: Cost spike > 5 standard deviations above baseline
- **Anomalous Increase**: Cost spike > 3 standard deviations above baseline

### Decreases
- **Critical Decrease**: Cost drop > 5σ below baseline (may indicate broken jobs)
- **Anomalous Decrease**: Cost drop > 3σ below baseline

### Insufficient History
< 30 days of data - baseline not yet established

## 🔍 Investigation Guide

### Critical Table Increases
**Root Causes:**
1. **Data volume spike** - More rows/partitions being processed
2. **Query complexity** - New expensive transformations added
3. **Duplicate runs** - Job ran multiple times due to retries
4. **Cluster misconfiguration** - Using wrong cluster size/type

**Actions:**
1. Check recent code changes to the asset
2. Verify partition range in job logs
3. Review Databricks job metrics (DBU consumption)
4. Compare with historical runs in Dagster

### Persistent Anomalies
Tables flagged anomalous for 3+ consecutive days indicate systemic issues, not one-time spikes.

**Common Causes:**
- Feature launch increasing workload
- Data source volume increase
- Performance regression in code
- Cluster autoscaling issues

## 💡 How to Use

**Daily:** Review critical anomalies (should be 0)
**Weekly:** Check persistent anomalies for patterns
**Monthly:** Analyze anomaly trends in summary view

## 🔗 Related Metrics

- Anomaly Summary → Overall health snapshot
- Attribution → Which team/product is affected
- Efficiency → Is increased cost justified by usage?

