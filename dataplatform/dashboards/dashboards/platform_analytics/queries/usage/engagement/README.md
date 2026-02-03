# User Engagement Scoring

Composite engagement metrics to identify platform health and user commitment.

## Overview

Engagement scores provide a single metric combining:
- **Recency** (40%): How recently and frequently they're active
- **Frequency** (30%): Query volume relative to peers
- **Breadth** (30%): Diversity of tables accessed

## Score Calculation

```
engagement_score = (recency * 0.4) + (frequency * 0.3) + (breadth * 0.3)
```

Where:
- `recency = active_days / 30` (days active in last month)
- `frequency = user_queries / max_queries` (normalized query volume)
- `breadth = user_tables / max_tables` (normalized table diversity)

## Engagement Tiers

**High Engagement** (>0.70)
- Platform champions
- Daily/weekly active
- Access diverse data
- Feature advocates

**Medium Engagement** (0.40-0.70)
- Solid core users
- Regular but not daily
- Focused use cases
- Growth targets

**Low Engagement** (<0.40)
- Sporadic usage
- Limited data access
- At-risk users
- Training candidates

## Applications

**Prioritization**
- Support: High engagement users get priority
- Features: Medium engagement users need activation
- Training: Low engagement users need help

**Benchmarking**
- Track team average engagement over time
- Compare departments
- Measure impact of new features
- Set minimum engagement thresholds

**Alerts**
- High engagement user becomes inactive (churn risk)
- Department average drops (adoption issue)
- New table has low engagement (discoverability problem)

## Target Metrics

- Team average engagement: >0.55
- >30% of users in high engagement tier
- <15% of users in low engagement tier
- Engagement score trending up MoM

