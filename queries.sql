-- Query 1: Record Count Comparison
-- Checks if the number of records match between source and target

SELECT 'source' AS system, COUNT(*) AS total_records
FROM `data-reconciliation-project.data_recon_project.source_transactions`
UNION ALL
SELECT 'target', COUNT(*)
FROM `data-reconciliation-project.data_recon_project.target_transactions`;


-- Query 2: Missing Records
-- Transactions present in source but missing in target
-- Root cause: pipeline drop or ingestion failure

SELECT s.transaction_id, s.user_id, s.amount, s.status, s.created_at
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
LEFT JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL
ORDER BY s.created_at;


-- Query 3: Duplicate Records
-- Transaction IDs appearing more than once in target
-- Root cause: retry logic re-ingesting the same record

SELECT transaction_id, COUNT(*) AS occurrence_count
FROM `data-reconciliation-project.data_recon_project.target_transactions`
GROUP BY transaction_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;


-- Query 4: Amount Mismatches
-- Same transaction, different amounts between source and target
-- Root cause: transformation or rounding error

SELECT
    s.transaction_id,
    s.user_id,
    s.amount AS source_amount,
    t.amount AS target_amount,
    ROUND(ABS(s.amount - t.amount), 2) AS discrepancy,
    ROUND(ABS(s.amount - t.amount) / s.amount * 100, 2) AS discrepancy_pct
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
WHERE ROUND(s.amount, 2) != ROUND(t.amount, 2)
ORDER BY discrepancy_pct DESC;


-- Query 5: Status Mismatches
-- Same transaction, different status between source and target
-- Root cause: out-of-order processing or sync delay

SELECT
    s.transaction_id,
    s.user_id,
    s.status AS source_status,
    t.status AS target_status,
    s.created_at AS source_time,
    t.updated_at AS target_time
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
WHERE s.status != t.status
ORDER BY s.created_at;


-- Query 6: High Value Amount Mismatches
-- Filters amount mismatches on transactions above 1000
-- Helps prioritise which discrepancies need urgent attention

SELECT
    s.transaction_id,
    s.user_id,
    s.amount AS source_amount,
    t.amount AS target_amount,
    ROUND(ABS(s.amount - t.amount), 2) AS discrepancy
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
WHERE ROUND(s.amount, 2) != ROUND(t.amount, 2)
AND s.amount > 1000
ORDER BY discrepancy DESC;


-- Query 7: User Level Summary
-- Shows which users are affected and by which issue types
-- Useful for investigating whether issues are user specific or systemic

SELECT
    s.user_id,
    COUNT(DISTINCT CASE WHEN t.transaction_id IS NULL THEN s.transaction_id END) AS missing_count,
    COUNT(DISTINCT CASE WHEN ROUND(s.amount,2) != ROUND(t.amount,2) THEN s.transaction_id END) AS amount_mismatch_count,
    COUNT(DISTINCT CASE WHEN s.status != t.status THEN s.transaction_id END) AS status_mismatch_count
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
LEFT JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
GROUP BY s.user_id
HAVING missing_count > 0
OR amount_mismatch_count > 0
OR status_mismatch_count > 0
ORDER BY missing_count DESC;


-- Query 8: Full Summary Report
-- Combines all issue types into a single output for reporting
-- Root causes documented per issue type above

SELECT 'MISSING_IN_TARGET' AS issue_type,
    s.transaction_id,
    s.user_id,
    CAST(s.amount AS STRING) AS source_amount,
    CAST(NULL AS STRING) AS target_amount,
    s.status AS source_status,
    CAST(NULL AS STRING) AS target_status
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
LEFT JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL

UNION ALL

SELECT 'DUPLICATE_IN_TARGET',
    transaction_id,
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING)
FROM `data-reconciliation-project.data_recon_project.target_transactions`
GROUP BY transaction_id
HAVING COUNT(*) > 1

UNION ALL

SELECT 'AMOUNT_MISMATCH',
    s.transaction_id,
    s.user_id,
    CAST(s.amount AS STRING),
    CAST(t.amount AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING)
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
WHERE ROUND(s.amount, 2) != ROUND(t.amount, 2)

UNION ALL

SELECT 'STATUS_MISMATCH',
    s.transaction_id,
    s.user_id,
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    s.status,
    t.status
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
WHERE s.status != t.status

ORDER BY issue_type, transaction_id;
