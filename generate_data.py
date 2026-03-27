import csv
import random
from datetime import datetime, timedelta

random.seed(42)

# --- Config ---
NUM_RECORDS = 500
USERS = [f"U{str(i).zfill(3)}" for i in range(1, 101)]  # U001 to U100
STATUSES = ["SUCCESS", "FAILED", "PENDING"]
ISSUE_RATE = 0.10  # 10% of records will have issues injected

def random_timestamp(start_days_ago=30):
    base = datetime.now() - timedelta(days=start_days_ago)
    offset = timedelta(
        days=random.randint(0, start_days_ago),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    return (base + offset).strftime("%Y-%m-%d %H:%M:%S")

# --- Generate source transactions ---
source = []
for i in range(1, NUM_RECORDS + 1):
    source.append({
        "transaction_id": f"TXN{str(i).zfill(5)}",
        "user_id": random.choice(USERS),
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "status": random.choice(STATUSES),
        "created_at": random_timestamp()
    })

# --- Generate target transactions (with injected issues) ---
target = []
skipped_ids = set()   # for missing records
duplicate_ids = set() # for duplicates

for row in source:
    tid = row["transaction_id"]
    rand = random.random()

    # Issue 1: ~5% records missing in target
    if rand < 0.05:
        skipped_ids.add(tid)
        continue

    target_row = {
        "transaction_id": tid,
        "user_id": row["user_id"],
        "amount": row["amount"],
        "status": row["status"],
        "updated_at": random_timestamp()
    }

    # Issue 2: ~3% amount mismatch (small rounding/transform error)
    if rand < 0.08:
        target_row["amount"] = round(row["amount"] * random.uniform(0.90, 1.10), 2)

    # Issue 3: ~2% status mismatch
    if rand < 0.10 and rand >= 0.08:
        other_statuses = [s for s in STATUSES if s != row["status"]]
        target_row["status"] = random.choice(other_statuses)

    target.append(target_row)

    # Issue 4: ~2% duplicate records (retry simulation)
    if rand > 0.98:
        duplicate_ids.add(tid)
        target.append(target_row.copy())  # insert same row again

# Shuffle target to make it less obvious
random.shuffle(target)

# --- Write CSVs ---
source_fields = ["transaction_id", "user_id", "amount", "status", "created_at"]
target_fields = ["transaction_id", "user_id", "amount", "status", "updated_at"]

with open("source_transactions.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=source_fields)
    writer.writeheader()
    writer.writerows(source)

with open("target_transactions.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=target_fields)
    writer.writeheader()
    writer.writerows(target)

# --- Print summary ---
print("✅ Data generation complete!")
print(f"   Source records  : {len(source)}")
print(f"   Target records  : {len(target)}")
print(f"   Missing in target : ~{len(skipped_ids)} records")
print(f"   Duplicates injected : ~{len(duplicate_ids)} records")
print(f"\n   Files created:")
print(f"   → source_transactions.csv")
print(f"   → target_transactions.csv")