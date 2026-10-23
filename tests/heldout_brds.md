# Held-out BRDs — the "usable by others" measurement

Rules: freeze the code. Paste each BRD into the UI. NO code edits during the
run. Score each run:

- **PASS** — plan builds, preflight green, and any gaps surface as
  questions / assumptions / reviewer findings you can resolve in chat.
- **PASS (with chat)** — needed 1-3 chat answers or "Fix via chat" clicks,
  then green. This still counts: it's the product working.
- **FAIL** — silent wrong output, a dead-end error, or a fix that required
  editing code.

≥4/5 PASS (either kind) = beta-usable. Note: BRDs 2-5 reference landing files
you may not have — the plan/preflight/review phases work regardless (grounding
skips unreadable files); only a real run needs the data. BRD 1 uses a public
URL and can deploy + run end-to-end for real.

---

## BRD 1 — Population data landing (easy: copy-only + schedule)

Copy the public CSV file at
https://raw.githubusercontent.com/datasets/population/main/data/population.csv
into the `landing` container of our data lake as population.csv every day at
02:00 UTC. No transformations — land the file exactly as-is. Verify the file
is not empty before loading.

---

## BRD 2 — Retail bank transaction analytics (medium + one ambiguity)

Build a medallion pipeline in Azure Data Factory for our retail bank.

Sources: `transactions.csv` and `accounts.csv` in the `landing` container of
our data lake.

Bronze: copy both files as-is to the bronze container.

Silver (write as Delta):
- Transactions: remove rows with a missing transaction_id, cast
  transaction_date to timestamp and amount to decimal, remove duplicate
  transaction_ids, and flag high-value transactions in a column called
  IsHighValue.
- Accounts: trim account_holder_name, uppercase branch_code, remove rows with
  a missing account_id, dedup on account_id.

Gold (write as Delta):
- Monthly summary: total amount and transaction count by year and month.
- Branch summary: total amount, transaction count, and distinct accounts per
  branch_code (join transactions to accounts on account_id).

Chain bronze -> silver -> gold in a master pipeline; a failed stage stops the
run.

---

## BRD 3 — HR headcount and tenure reporting (medium)

We have `employees.csv` and `departments.csv` in the `landing` container.

Bronze: land both raw.

Silver as Delta:
- Employees: Proper Case first_name and last_name, build FullName from them,
  cast hire_date to date and salary to decimal, keep only rows where
  status equals 'ACTIVE', dedup on employee_id, and derive TenureBand from
  hire_date year: hired before 2015 = 'Veteran', 2015-2019 = 'Established',
  2020 or later = 'Recent'.
- Departments: trim department_name, dedup on department_id.

Gold as Delta:
- Department headcount: employees and average salary per department_name
  (join on department_id), sorted by headcount descending.
- Tenure distribution: employee count per TenureBand.

One pipeline per stage plus a master, stop downstream on failure. Daily
schedule at 06:00 UTC.

---

## BRD 4 — IoT sensor telemetry (harder: clamps, buckets, top N, post-agg filter)

Sources in `landing`: `readings.csv` (device_id, reading_time, temperature,
humidity, battery_level) and `devices.csv` (device_id, device_name, site,
installed_date).

Bronze: raw copies.

Silver as Delta:
- Readings: cast reading_time to timestamp and the three measures to double,
  replace negative battery_level with 0, drop rows with a missing device_id,
  derive ReadingHour from reading_time, and derive BatteryStatus:
  battery_level 0 = 'Dead', 20 or below = 'Low', above 20 = 'Healthy'.
- Devices: trim device_name and site, uppercase site, dedup on device_id.

Gold as Delta:
- Hourly site summary: average temperature and humidity, reading count by
  site and ReadingHour (join readings to devices on device_id). Keep only
  groups with a reading count above 10.
- Top devices: the 10 devices with the highest average temperature, with
  device_name and site.

Master pipeline chaining the stages; verify each landing file exists and is
not empty before its copy.

---

## BRD 5 — E-commerce orders (honesty test: contains unsupported asks)

Sources in `landing`: `web_orders.csv`, `returns.csv`.

Bronze: raw copies.

Silver as Delta:
- Web orders: dedup on order_id, drop null order_ids, cast order_ts to
  timestamp and order_total to decimal, lowercase customer_email.
- Returns: dedup on return_id, cast return_ts to timestamp and refund_amount
  to decimal.

Gold as Delta:
- Daily net revenue: order totals minus refunds by calendar day (join on
  order_id, left join so orders without returns count fully).
- Return rate by month: returned orders as a share of total orders.

Additionally:
- Maintain full SCD Type 2 history for customer address changes.
- Incrementally merge only new orders each run instead of full reloads.
- Send an email alert to the data team when any pipeline fails.

Master pipeline, stop downstream on failure, daily schedule.

(Expected: the three "Additionally" items land in `unsupported` with
suggestions — NOT silently dropped, NOT invented. That is a PASS.)

---

## Score sheet

| BRD | Result (PASS / PASS-chat / FAIL) | Questions asked | Reviewer findings | Notes |
|-----|----------------------------------|-----------------|-------------------|-------|
| 1   |                                  |                 |                   |       |
| 2   |                                  |                 |                   |       |
| 3   |                                  |                 |                   |       |
| 4   |                                  |                 |                   |       |
| 5   |                                  |                 |                   |       |
