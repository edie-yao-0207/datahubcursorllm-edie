# Data Cost & Efficiency (DCE) - SLI/SLO Framework

**Owner:** Firmware VDP Team  
**Last Updated:** December 2025  
**Dashboard Location:** Databricks SQL Dashboard - Data Cost & Efficiency

---

## Executive Summary

This document defines Service Level Indicators (SLIs) and Service Level Objectives (SLOs) for monitoring data platform cost efficiency and usage health. These metrics enable proactive management of infrastructure spend while ensuring data quality and user satisfaction.

---

## 1. Cost Management SLIs & SLOs

### 1.1 Monthly Cost Target

**SLI:** Total monthly data platform cost (USD)

**Measurement:**
```sql
SUM(total_cost) 
WHERE date >= DATE_TRUNC('month', CURRENT_DATE)
  AND asset_owner = 'firmwarevdp'
```

**SLO Thresholds:**
- **Target:** ≤ $12,000/month (80% of budget)
- **Warning:** $12,001 - $15,000/month (80-100% of budget)
- **Breach:** > $15,000/month (exceeds budget)

**Alert Conditions:**
- Warning: When cost exceeds $12k in current month
- Critical: When cost exceeds $15k in current month
- Forecast breach: When projected month-end cost > $15k

---

### 1.2 Month-over-Month Cost Growth

**SLI:** Percentage change in monthly cost vs prior month

**Measurement:**
```sql
(current_month_cost - prior_month_cost) / prior_month_cost
```

**SLO Thresholds:**
- **Healthy:** ≤ 10% MoM growth (0.10)
- **Concerning:** 10-20% MoM growth (0.10-0.20)
- **Critical:** > 20% MoM growth (0.20)

**Alert Conditions:**
- Warning: MoM growth > 15% for 2 consecutive months
- Critical: MoM growth > 25% in any month

---

### 1.3 Critical Cost Anomalies

**SLI:** Number of tables with critical cost increases

**Measurement:**
```sql
COUNT(*) WHERE daily.status = 'critical_increase'
  AND group_label = 'date, asset_owner, table'
```

**SLO Thresholds:**
- **Healthy:** 0-2 critical anomalies/day
- **Concerning:** 3-5 critical anomalies/day
- **Critical:** > 5 critical anomalies/day

**Alert Conditions:**
- Warning: 3+ critical anomalies persisting for 3+ days
- Critical: 5+ critical anomalies in a single day

---

### 1.4 Cost per Active User

**SLI:** Monthly cost divided by monthly active users

**Measurement:**
```sql
total_monthly_cost / monthly_unique_users
```

**SLO Thresholds:**
- **Efficient:** < $50/user/month (0-50)
- **Acceptable:** $50-$100/user/month (50-100)
- **Inefficient:** > $100/user/month (100+)

**Alert Conditions:**
- Warning: Cost per user > $75 for 2 consecutive months
- Critical: Cost per user > $125

---

### 1.5 Idle Table Cost

**SLI:** Monthly cost of tables with zero usage

**Measurement:**
```sql
SUM(monthly_cost) 
WHERE monthly_users = 0 
  AND monthly_cost > 10
```

**SLO Thresholds:**
- **Efficient:** < 5% of total cost (0.05)
- **Acceptable:** 5-10% of total cost (0.05-0.10)
- **Wasteful:** > 10% of total cost (0.10)

**Alert Conditions:**
- Warning: Idle cost > 7.5% of total for 2 consecutive months
- Critical: Single idle table costing > $500/month

---

## 2. Usage & Adoption SLIs & SLOs

### 2.1 Monthly Active Users (MAU)

**SLI:** Distinct users querying data platform in past 30 days

**Measurement:**
```sql
COUNT(DISTINCT email) 
WHERE date >= DATE_SUB(CURRENT_DATE, 30)
```

**SLO Thresholds:**
- **Healthy Growth:** > 10% MoM growth (0.10)
- **Stable:** -5% to +10% MoM change (-0.05 to 0.10)
- **Declining:** > 5% MoM decline (< -0.05)

**Alert Conditions:**
- Warning: MAU declines > 10% for 2 consecutive months
- Critical: MAU declines > 20% in any month

---

### 2.2 Weekly Active Users (WAU)

**SLI:** Distinct users querying data platform in past 7 days

**Measurement:**
```sql
COUNT(DISTINCT email) 
WHERE date >= DATE_SUB(CURRENT_DATE, 7)
```

**SLO Thresholds:**
- **Healthy:** WAU/MAU ratio > 0.60 (weekly engagement)
- **Acceptable:** WAU/MAU ratio 0.40-0.60
- **Low Engagement:** WAU/MAU ratio < 0.40

**Alert Conditions:**
- Warning: WAU/MAU ratio < 0.45 for 3 consecutive weeks
- Critical: WAU/MAU ratio < 0.30

---

### 2.3 New User Onboarding

**SLI:** New users (first query) in past 30 days

**Measurement:**
```sql
COUNT(DISTINCT email) 
WHERE first_query_date >= DATE_SUB(CURRENT_DATE, 30)
```

**SLO Thresholds:**
- **Healthy:** ≥ 5 new users/month
- **Acceptable:** 2-4 new users/month
- **Stagnant:** < 2 new users/month

**Alert Conditions:**
- Warning: Zero new users for 45 consecutive days
- Critical: Negative net user growth for 2 consecutive months

---

### 2.4 Power User Concentration

**SLI:** Percentage of queries from top 10% users

**Measurement:**
```sql
SUM(queries from top 10% users) / SUM(all queries)
```

**SLO Thresholds:**
- **Healthy Distribution:** < 60% from top 10% (0.60)
- **Moderate Concentration:** 60-80% from top 10% (0.60-0.80)
- **High Concentration:** > 80% from top 10% (0.80)

**Alert Conditions:**
- Warning: > 75% concentration for 2 consecutive months
- Critical: > 90% concentration (single point of failure risk)

---

### 2.5 Table Access Distribution

**SLI:** Tables accessed by < 3 users per month

**Measurement:**
```sql
COUNT(*) WHERE monthly_users < 3 
  AND monthly_cost > 50
```

**SLO Thresholds:**
- **Healthy:** < 10% of tables are underutilized (0.10)
- **Acceptable:** 10-20% of tables underutilized (0.10-0.20)
- **Concerning:** > 20% of tables underutilized (0.20)

**Alert Conditions:**
- Warning: Underutilized % > 15% for 3 consecutive months
- Critical: High-cost table (>$500/mo) with < 3 users

---

## 3. Efficiency SLIs & SLOs

### 3.1 Cost per 1K Queries

**SLI:** Cost per thousand queries executed

**Measurement:**
```sql
(total_monthly_cost / total_monthly_queries) * 1000
```

**SLO Thresholds:**
- **Efficient:** < $1 per 1K queries (1.0)
- **Acceptable:** $1-$3 per 1K queries (1.0-3.0)
- **Inefficient:** > $3 per 1K queries (3.0)

**Alert Conditions:**
- Warning: Cost per 1K queries > $2.50 for 2 consecutive months
- Critical: Cost per 1K queries > $5

---

### 3.2 Efficiency Degradation

**SLI:** Tables where cost increases while usage decreases

**Measurement:**
```sql
COUNT(*) 
WHERE weekly_cost_delta > 0 
  AND weekly_user_delta < 0
```

**SLO Thresholds:**
- **Healthy:** < 5 degrading tables/week
- **Concerning:** 5-10 degrading tables/week
- **Critical:** > 10 degrading tables/week

**Alert Conditions:**
- Warning: Same table degrading for 3+ consecutive weeks
- Critical: Table cost up 50%+ while users down 50%+

---

## 4. Data Quality SLIs & SLOs

### 4.1 Overall DQ Pass Rate

**SLI:** Percentage of data quality checks passing

**Measurement:**
```sql
SUM(passed_checks) / SUM(total_checks)
```

**SLO Thresholds:**
- **Excellent:** ≥ 99% pass rate (0.99)
- **Good:** 95-99% pass rate (0.95-0.99)
- **Poor:** < 95% pass rate (0.95)

**Alert Conditions:**
- Warning: Pass rate < 97% for 3 consecutive days
- Critical: Pass rate < 90% in any day

---

### 4.2 Critical DQ Degradation

**SLI:** Tables with critical DQ pass rate decline

**Measurement:**
```sql
COUNT(*) WHERE daily.status = 'critical_degradation'
```

**SLO Thresholds:**
- **Healthy:** 0-2 critical degradations/day
- **Concerning:** 3-5 critical degradations/day
- **Critical:** > 5 critical degradations/day

**Alert Conditions:**
- Warning: Same table degrading for 3+ consecutive days
- Critical: Production table with < 80% pass rate

---

### 4.3 Chronic Failure Rate

**SLI:** DQ checks failing for 7+ consecutive days

**Measurement:**
```sql
COUNT(*) WHERE consecutive_failures >= 7
```

**SLO Thresholds:**
- **Healthy:** 0 chronic failures
- **Acceptable:** 1-2 chronic failures
- **Critical:** > 2 chronic failures

**Alert Conditions:**
- Warning: Any check failing for 7+ consecutive days
- Critical: Production table check failing for 14+ days

---

## 5. Leading Indicators

### 5.1 Anomaly Detection Coverage

**SLI:** Percentage of tables with anomaly monitoring

**Measurement:**
```sql
COUNT(DISTINCT table in fct_table_cost_anomalies) / 
COUNT(DISTINCT table in fct_table_cost_daily)
```

**Target:** 100% coverage for tables > $50/month

---

### 5.2 Alert Response Time

**SLI:** Median time from alert to resolution

**Target:** 
- P1 (Critical): < 4 hours
- P2 (Warning): < 24 hours
- P3 (Informational): < 7 days

---

### 5.3 Cost Forecast Accuracy

**SLI:** Accuracy of month-end cost projections

**Measurement:**
```sql
ABS(projected_cost - actual_cost) / actual_cost
```

**Target:** < 10% error at 50% through month

---

## 6. Review Cadence

### Daily Reviews (Automated)
- Critical cost anomalies
- DQ pass rate
- Idle table costs > $100/day

### Weekly Reviews (Team)
- Cost vs budget tracking
- Top cost drivers
- Efficiency degradation trends
- New anomalies

### Monthly Reviews (Leadership)
- SLO compliance summary
- Month-over-month trends
- Budget forecast
- Action items and improvements

---

## 7. Escalation Paths

### Warning Level
- **Owner:** Data Platform Team
- **Response:** Investigate within 24 hours
- **Action:** Create ticket, add to backlog

### Critical Level
- **Owner:** Data Platform Lead
- **Response:** Immediate investigation
- **Action:** 
  - P1 for cost breach or DQ failure
  - Notify stakeholders within 1 hour
  - Root cause analysis within 24 hours

### Sustained Issues (3+ consecutive periods)
- **Owner:** Engineering Manager
- **Action:** 
  - Quarterly planning item
  - Architecture review
  - Process improvement

---

## 8. Continuous Improvement

### Quarterly Goals
- Reduce cost per user by 10% YoY
- Maintain > 98% DQ pass rate
- Keep idle table cost < 5% of total
- Increase MAU by 15% YoY

### Success Metrics
- Zero budget breaches
- Zero production DQ failures > 7 days
- < 5% of alerts require escalation
- 100% of critical alerts resolved within SLA

---

## Appendix: Metric Definitions

**Cost Metrics:**
- `total_cost`: Sum of Databricks + AWS costs
- `dbx_cost`: Databricks DBU costs
- `aws_cost`: AWS compute costs (EC2, storage, etc.)

**Usage Metrics:**
- `unique_users`: COUNT(DISTINCT email)
- `total_queries`: COUNT(*) from query logs
- `active_days`: Days with at least 1 query

**Time Periods:**
- Daily: 1 day
- Weekly: 7 days rolling
- Monthly: 30 days rolling
- Quarterly: 90 days rolling
- Yearly: 365 days rolling

**Anomaly Statuses:**
- `normal`: Within 2 sigma of baseline
- `warning`: 2-3 sigma from baseline
- `anomalous_increase/decrease`: > 3 sigma
- `critical_increase/decrease`: > 4 sigma
- `degrading`: Sustained negative trend (3+ periods)

---

**Questions or Updates?**  
Contact: Firmware VDP Team  
Slack: #data-platform

