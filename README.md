# Transaction Data Reconciliation & Data Quality Framework (SQL)

## 📌 Project Overview
This project simulates a data reconciliation framework to validate transaction data between source and target systems. It identifies common data quality issues such as missing records, duplicates, and mismatches.

---

## 🧩 Scenario
In real-world systems, transaction data flows from APIs to databases or data warehouses. Due to failures or delays, inconsistencies can occur.  
This project models such a scenario using two datasets:
- Source transactions (simulating API/system data)
- Target transactions (simulating stored database records)

---

## 🗂️ Tables

### Source Table: `source_transactions`
- transaction_id  
- user_id  
- amount  
- status  
- created_at  

### Target Table: `target_transactions`
- transaction_id  
- user_id  
- amount  
- status  
- updated_at  

---

## 🔍 Data Quality Checks

The following validations were implemented:

1. Missing records in target  
2. Duplicate records in target  
3. Amount mismatches between source and target  
4. Status mismatches between source and target  

---

## 📊 Sample Output

| Issue Type           | Transaction ID |
|---------------------|----------------|
| MISSING_IN_TARGET   | T4             |
| DUPLICATE_IN_TARGET | T7             |
| AMOUNT_MISMATCH     | T5             |
| STATUS_MISMATCH     | T6             |

---

## 💡 Key Insights

- Missing records indicate potential pipeline failures or delays  
- Duplicate records suggest retry or ingestion issues  
- Amount mismatches highlight data transformation errors  
- Status mismatches show inconsistencies between systems  

---

## 🛠️ Recommendations

- Implement validation checks during data ingestion  
- Add deduplication logic to prevent duplicate records  
- Monitor pipelines for missing or delayed data  
- Ensure consistent status updates across systems  

---

## ⚙️ Tech Used
- SQL (Google BigQuery)
