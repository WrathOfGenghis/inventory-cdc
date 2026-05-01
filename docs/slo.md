# Service-Level Objectives

This document mirrors §18 of the design report and should be the source
of truth for the on-call team.

## The SLOs

| SLO | Definition | Target | Window |
| --- | --- | --- | --- |
| Freshness | `histogram_quantile(0.95, rate(sync_latency_seconds_bucket[28d]))` | ≤ 3.0 s | Rolling 28 days |
| Critical freshness | p99 of the same metric | ≤ 5.0 s | Rolling 28 days |
| Successful processing | `applied_total / received_total` | ≥ 99.95 % | Rolling 28 days |
| Read-model availability | Storefront API success rate against `website_inventory` | ≥ 99.9 % | Rolling 30 days |
| Mismatch rate | `inventory_mismatch_total / 1000` | ≤ 0.1 % | Daily |

## SLA toward the storefront team

| Promise | Threshold | If breached |
| --- | --- | --- |
| Sync freshness | p95 ≤ 5 s, p99 ≤ 10 s | RCA within 5 business days |
| Read availability | 99.5 % monthly | Service credit per 0.1 % missed |
| No silent loss | Zero committed CDC events unaccounted | SEV-1 incident |

## Error-budget arithmetic

99.95 % successful processing → 0.05 % budget. At 5 M events / day the
budget is **2,500 events / day**, or about **70,000 events** over a
28-day window.

Multi-window burn alerts (Google SRE pattern):

| Window | Threshold | Severity |
| --- | --- | --- |
| 1 h burn > 14.4× and 5 m burn > 14.4× | Page (fast burn) |
| 6 h burn > 6× and 30 m burn > 6× | Page (medium burn) |
| 24 h burn > 3× and 2 h burn > 3× | Ticket |
| 72 h burn > 1× and 6 h burn > 1× | Ticket |

A burn rate of 14.4× will exhaust the 28-day budget in 2 days.

## Policy when the budget is exhausted

These actions are not optional:

1. Feature work on the inventory pipeline pauses. Engineering capacity
   moves to reliability work.
2. Non-emergency configuration changes require a second reviewer.
3. Deploys are restricted to working hours with on-call awake.
4. A blameless retrospective is scheduled within 5 business days.
5. The next sprint must include at least one item from the
   *reliability backlog*.

## Escalation matrix

| Severity | Trigger | Page | Response |
| --- | --- | --- | --- |
| SEV-1 | Pipeline halted > 10 min OR mismatch on critical SKUs OR overselling rate alert | Primary → secondary → manager | < 5 min ack, < 30 min mitigation |
| SEV-2 | p95 latency > 3 s for 10 min OR DLQ > 50 in 5 min OR consumer lag > 50 k for 10 min | Primary on-call | < 15 min ack, < 2 h mitigation |
| SEV-3 | Schema warning OR rising error-budget burn (> 3×) | Slack ticket | Same business day |
