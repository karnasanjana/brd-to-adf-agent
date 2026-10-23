# Stress suite — 10 BRDs, one per untested dimension

Dimensions: HTTP+medallion, join width/scale, copy-only multi-file, text/binary
formats, new filter ops (IN/BETWEEN/LIKE/regex), regex cleaning ops, ambiguity
(must ASK), unsupported-heavy (must REFUSE), custom containers + deploy target,
messy informal wording. BRD 7 is expected to ask a question, not build.

---

## BRD 1 — HTTP source with full medallion (transform an internet file)

Pull the CSV at
https://raw.githubusercontent.com/datasets/population/main/data/population.csv
and land it as population.csv in the `landing` container. Bronze: copy
landing/population.csv raw. Silver (Delta): cast Year to integer and Value to
long, keep only rows where Year >= 2000, dedup on Country Name plus Year.
Gold (Delta): total population value per Country Name across all years,
sorted descending, keep the top 20. Chain landing -> bronze -> silver ->
gold, stop on failure.

---

## BRD 2 — Wide multi-entity join at scale (bike store data)

Sources in the landing container: orders.csv, order_items.csv, products.csv,
categories.csv, brands.csv, stores.csv.

Bronze: copy all six raw. Silver (Delta): cast ids to integer and money
columns to decimal on every entity; cast order_date to timestamp; dedup each
entity on its id; drop rows with null ids. Gold (Delta): one revenue_analysis
output joining orders -> order_items -> products -> categories -> brands ->
stores (orders to stores on store_id), grouped by category_name, brand_name,
store_name and order year, with Revenue = sum of quantity * list_price *
(1 - discount), UnitsSold, and OrderCount. Master pipeline chains the stages.

---

## BRD 3 — Copy-only, multiple files, weekly schedule

From the landing container copy brands.csv, categories.csv, and stores.csv
byte-for-byte into the bronze container. Verify each file exists and is not
empty first. No transformations at all. Run every Monday at 05:00 UTC.

---

## BRD 4 — Text and binary landing (no-transform formats)

Our landing container has events.log (plain text) and snapshot.bin (binary).
Copy both to bronze exactly as-is — byte-for-byte, no parsing, no
transformation. Nothing downstream. Manual trigger only.

---

## BRD 5 — Filter operator coverage (IN / BETWEEN / LIKE)

Source: transactions.csv in landing. Bronze raw. Silver (Delta): cast
transaction_date to timestamp and amount to decimal; keep only rows where
amount is between 100 and 5000, description is like 'purchase%', and
account_id is in the list A01,A02,A03,A04; drop null transaction_ids; dedup
on transaction_id. Gold (Delta): transaction count and total amount per year
and month.

---

## BRD 6 — Regex and special-character cleaning

Source: accounts.csv in landing. Bronze raw. Silver (Delta): trim
account_holder_name and remove special characters from it, use a regex
replace to strip the 'br-' prefix off branch_code, replace blank or empty
branch_code values with 'unknown', uppercase branch_code, dedup on
account_id. Gold (Delta): account count per branch_code.

---

## BRD 7 — Ambiguity trap (correct answer is a QUESTION)

Build customer analytics from our customer data. Clean it up appropriately
and produce useful KPIs for the business.

---

## BRD 8 — Unsupported-heavy (correct answer is honest refusal + build the rest)

Source: readings.csv in landing. Copy it to bronze raw. Then:
- Stream the readings in real time from Azure Event Hub.
- Loop over each daily file with a ForEach activity and process them in
  parallel batches.
- Pivot the readings so each device becomes a column.
- Run a Databricks notebook to score anomalies.
- Send a Teams message when anomalies exceed 100 per hour.
The bronze copy must still be built and runnable regardless.

---

## BRD 9 — Custom containers + named deploy target + hourly schedule

Reuse resource group RetailProject, data factory RetailADFFF, storage account
retailproject3001. Sources: employees.csv and departments.csv in landing.
Land raw copies in the staging container, write cleaned Delta (dedup on ids,
drop null ids, cast salary to decimal and hire_date to date, trim names) to
the refined container, and write one gold Delta output (headcount and average
salary per department_name, joined on department_id) to the reporting
container. Chain the stages in a master pipeline and run it every hour.

---

## BRD 10 — Informal wording (robustness)

hey so i got web_orders.csv and returns.csv sitting in landing.

bronze: copy both files as-is.

silver (delta):
- web_orders: dedupe on order_id, drop rows with null order_id, cast
  order_ts to timestamp, cast order_total to decimal, lowercase email.
- returns: dedupe on return_id, cast return_ts to timestamp, cast
  refund_amount to decimal.

gold (delta), two outputs:
- daily_net_revenue: left join web_orders to returns on order_id (orders
  without returns still count, refund treated as 0), group by order date,
  net_revenue = sum(order_total) - sum(refund_amount).
- monthly_return_rate: group by order month, return_rate = count of
  distinct returned order_ids / count of distinct order_ids.

everything delta. schedule the whole thing daily at 7am UTC. thx
