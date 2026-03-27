# Transaction Data Reconciliation Framework

## What this project does

When data moves between systems, things break. Records go missing, duplicates sneak in, amounts don't match up, statuses get out of sync. This project is a framework to catch all of that.

I built it around a payment transaction scenario where data flows from a source system into a target database, and the goal is to figure out exactly where and how the data broke.

---

## The Dataset

I couldn't use real transaction data, so I wrote a Python script (generate_data.py) to generate a realistic one. It creates 500 source transactions and a target dataset with real failure patterns baked in:

| Issue | Rate | Why it happens |
|---|---|---|
| Missing records | ~5% | Pipeline drops, ingestion failures |
| Duplicates | ~2% | Retry logic ingesting the same record twice |
| Amount mismatches | ~3% | Rounding errors in transformation |
| Status mismatches | ~2% | Out-of-order processing between systems |

---

## Findings Overview

Before diving into individual checks, here is how the two systems compare at a high level:

    SELECT 'source' AS system, COUNT(*) AS total_records
    FROM source_transactions
    UNION ALL
    SELECT 'target', COUNT(*)
    FROM target_transactions;

Result:

| system | total_records |
|---|---|
| source | 500 |
| target | 497 |

Three records already unaccounted for just from the count. The detailed checks below show exactly where the gaps are.

---

## Check 1: Missing Records

Transactions that exist in source but never made it to target.

    SELECT s.transaction_id, s.user_id, s.amount, s.status, s.created_at
    FROM source_transactions s
    LEFT JOIN target_transactions t ON s.transaction_id = t.transaction_id
    WHERE t.transaction_id IS NULL
    ORDER BY s.created_at;

Sample output:

| transaction_id | user_id | amount | status | created_at |
|---|---|---|---|---|
| TXN00265 | U029 | 1672.01 | SUCCESS | 2026-02-27 23:20:07 |
| TXN00276 | U042 | 3028.98 | PENDING | 2026-03-01 08:08:06 |
| TXN00321 | U007 | 4330.33 | FAILED | 2026-03-05 10:31:23 |

17 transactions total never reached the target. That is a 3.4% drop rate. In a real payments pipeline this would trigger an immediate investigation into where in the ingestion process records are being lost.

---

## Check 2: Duplicate Records

Transaction IDs appearing more than once in target, usually caused by retry logic.

    SELECT transaction_id, COUNT(*) AS occurrence_count
    FROM target_transactions
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
    ORDER BY occurrence_count DESC;

Sample output:

| transaction_id | occurrence_count |
|---|---|
| TXN00227 | 2 |
| TXN00445 | 2 |
| TXN00304 | 2 |

14 duplicates found, every single one appearing exactly twice. That pattern is consistent with a single retry attempt after a failed ingestion, not a random bug. Deduplication logic at the ingestion layer would prevent this.

---

## Check 3: Amount Mismatches

Same transaction, different amounts between source and target.

    SELECT
        s.transaction_id,
        s.user_id,
        s.amount AS source_amount,
        t.amount AS target_amount,
        ROUND(ABS(s.amount - t.amount), 2) AS discrepancy,
        ROUND(ABS(s.amount - t.amount) / s.amount * 100, 2) AS discrepancy_pct
    FROM source_transactions s
    JOIN target_transactions t ON s.transaction_id = t.transaction_id
    WHERE ROUND(s.amount, 2) != ROUND(t.amount, 2)
    ORDER BY discrepancy_pct DESC;

Sample output:

| transaction_id | user_id | source_amount | target_amount | discrepancy | discrepancy_pct |
|---|---|---|---|---|---|
| TXN00440 | U023 | 3133.71 | 3420.12 | 286.41 | 9.14 |
| TXN00111 | U042 | 1694.13 | 1545.21 | 148.92 | 8.79 |
| TXN00365 | U077 | 4412.87 | 4775.81 | 362.94 | 8.22 |

8 mismatches found with discrepancies up to 9.14%. Every single one was on a transaction above 1000 in value. The discrepancy percentage column makes it easy to prioritise which ones need urgent attention.

---

## Check 4: Status Mismatches

Same transaction, different status between source and target.

    SELECT
        s.transaction_id,
        s.user_id,
        s.status AS source_status,
        t.status AS target_status,
        s.created_at AS source_time,
        t.updated_at AS target_time
    FROM source_transactions s
    JOIN target_transactions t ON s.transaction_id = t.transaction_id
    WHERE s.status != t.status
    ORDER BY s.created_at;

Sample output:

| transaction_id | user_id | source_status | target_status | source_time | target_time |
|---|---|---|---|---|---|
| TXN00169 | U091 | PENDING | SUCCESS | 2026-02-26 00:06:05 | 2026-02-28 10:47:00 |
| TXN00356 | U059 | SUCCESS | PENDING | 2026-03-02 21:06:30 | 2026-03-02 08:34:18 |

16 mismatches found. TXN00356 is interesting because the target was updated before the source timestamp, meaning the target processed the transaction before the source even recorded it. That is an out-of-order processing issue, which is a different root cause than a simple sync delay and needs a different fix.

---

## Full Summary

All issue types combined into a single output for reporting:

| issue_type | Records |
|---|---|
| AMOUNT_MISMATCH | 8 |
| DUPLICATE_IN_TARGET | 14 |
| MISSING_IN_TARGET | 17 |
| STATUS_MISMATCH | 16 |
| Total | 55 |

---

## Files

generate_data.py — builds the synthetic dataset with failure patterns injected

queries.sql — all 8 validation queries with comments explaining each one

source_transactions.csv — the source dataset

target_transactions.csv — the target dataset

results/ — BigQuery screenshots for every query

---

## Tools

Python, SQL, Google BigQuery
