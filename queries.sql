-- Create Source Table
CREATE OR REPLACE TABLE `data-reconciliation-project.data_recon_project.source_transactions` AS
SELECT 'T1' AS transaction_id, 'U1' AS user_id, 500 AS amount, 'SUCCESS' AS status, CURRENT_TIMESTAMP() AS created_at UNION ALL
SELECT 'T2', 'U2', 300, 'SUCCESS', CURRENT_TIMESTAMP() UNION ALL
SELECT 'T3', 'U3', 200, 'FAILED', CURRENT_TIMESTAMP() UNION ALL
SELECT 'T4', 'U4', 150, 'SUCCESS', CURRENT_TIMESTAMP() UNION ALL
SELECT 'T5', 'U5', 700, 'SUCCESS', CURRENT_TIMESTAMP() UNION ALL
SELECT 'T6', 'U6', 400, 'PENDING', CURRENT_TIMESTAMP() UNION ALL
SELECT 'T7', 'U7', 250, 'SUCCESS', CURRENT_TIMESTAMP();

-- Create Target Table (with issues)
CREATE OR REPLACE TABLE `data-reconciliation-project.data_recon_project.target_transactions` AS
SELECT 'T1' AS transaction_id, 'U1' AS user_id, 500 AS amount, 'SUCCESS' AS status, CURRENT_TIMESTAMP() AS updated_at UNION ALL
SELECT 'T2', 'U2', 300, 'SUCCESS', CURRENT_TIMESTAMP() UNION ALL
SELECT 'T3', 'U3', 200, 'FAILED', CURRENT_TIMESTAMP() UNION ALL
SELECT 'T5', 'U5', 650, 'SUCCESS', CURRENT_TIMESTAMP() UNION ALL
SELECT 'T6', 'U6', 400, 'FAILED', CURRENT_TIMESTAMP() UNION ALL
SELECT 'T7', 'U7', 250, 'SUCCESS', CURRENT_TIMESTAMP() UNION ALL
SELECT 'T7', 'U7', 250, 'SUCCESS', CURRENT_TIMESTAMP();

-- Missing Records Check
SELECT s.transaction_id
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
LEFT JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- Duplicate Records Check
SELECT transaction_id, COUNT(*) as record_count
FROM `data-reconciliation-project.data_recon_project.target_transactions`
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- Amount Mismatch Check
SELECT 
    s.transaction_id,
    s.amount AS source_amount,
    t.amount AS target_amount
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
WHERE s.amount != t.amount;

-- Status Mismatch Check
SELECT 
    s.transaction_id,
    s.status AS source_status,
    t.status AS target_status
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
WHERE s.status != t.status;

-- Final Summary
SELECT 'MISSING_IN_TARGET' AS issue_type, s.transaction_id
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
LEFT JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL

UNION ALL

SELECT 'DUPLICATE_IN_TARGET', transaction_id
FROM `data-reconciliation-project.data_recon_project.target_transactions`
GROUP BY transaction_id
HAVING COUNT(*) > 1

UNION ALL

SELECT 'AMOUNT_MISMATCH', s.transaction_id
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
WHERE s.amount != t.amount

UNION ALL

SELECT 'STATUS_MISMATCH', s.transaction_id
FROM `data-reconciliation-project.data_recon_project.source_transactions` s
JOIN `data-reconciliation-project.data_recon_project.target_transactions` t
ON s.transaction_id = t.transaction_id
WHERE s.status != t.status;
