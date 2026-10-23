# Gold/silver op-coverage suite — 10 BRDs, real s-landing data

All ten point at files that actually exist in the `s-landing` container
(account `azurejaydeltalake`), so every BRD can be planned, built, AND run
for real — not just plan-phase grounding. Two source datasets live there:

- **bike-store**: customers.csv, orders.csv, order_items.csv, products.csv,
  categories.csv, brands.csv, stores.csv, staffs.csv, stocks.csv
- **olist** (Brazilian e-commerce): olist_customers_dataset.csv,
  olist_orders_dataset.csv, olist_order_items_dataset.csv,
  olist_order_payments_dataset.csv, olist_products_dataset.csv

Each BRD is a full bronze -> silver -> gold medallion but stresses a
different corner of the op vocabulary so that across all ten, every
cleaning op, filter op, and aggregation op gets exercised at least once.
(IN/BETWEEN/LIKE/regex filters and the ambiguity/refuse cases already have
dedicated coverage in `stress_brds.md` — this suite leans toward gold-side
variety: multi-way joins, every aggregation function, sort+top_n, and
post-aggregation `case` buckets.)

---

## BRD 1 — Bike-store customer geography (name cleaning + blank-default)

Source: `customers.csv` in the `s-landing` container.

Bronze: copy raw.

Silver (Delta): trim first_name and last_name, Proper Case both, build a
FullName column by concatenating them with a space, replace blank or NULL
phone values with 'Unknown', uppercase state, drop rows with a missing
customer_id or with zip_code equal to '00000', dedup on customer_id.

Gold (Delta): customer count and distinct city count per state, sorted by
customer count descending, top 10 states.

---

## BRD 2 — Bike-store order fulfillment by store/staff (3-way join, post-agg case)

Sources: `orders.csv`, `staffs.csv`, `stores.csv` in `s-landing`.

Bronze: copy all three raw.

Silver (Delta):
- Orders: cast order_date, required_date, and shipped_date to date; keep
  only orders with order_date between 2016-01-01 and 2018-12-31; drop rows
  with a missing staff_id; bucket order_status into OrderStatusLabel: 1 ->
  'Pending', 2 -> 'Processing', 3 -> 'Rejected', 4 -> 'Completed', otherwise
  'Unknown'; dedup on order_id.
- Staffs: trim and Proper Case first_name/last_name; keep only rows where
  active equals 1.
- Stores: trim store_name, uppercase state.

Gold (Delta): fulfillment_by_store — left join orders to staffs on
staff_id, left join to stores on store_id; order count and distinct staff
count per store_name. After aggregation, bucket stores into VolumeSegment:
order count >= 200 -> 'High', otherwise 'Low'. Sort by order count
descending.

---

## BRD 3 — Bike-store revenue by brand/category (4-way join, arithmetic, sum/avg)

Sources: `order_items.csv`, `products.csv`, `categories.csv`, `brands.csv`
in `s-landing`.

Bronze: copy all four raw.

Silver (Delta):
- Order items: cast quantity to integer, list_price and discount to
  decimal; drop rows where discount is not less than 1; derive LineTotal =
  quantity * list_price * (1 - discount); drop rows with a missing
  product_id.
- Products: trim product_name; keep only rows where category_id is in the
  list 1,2,5,6,7; cast list_price to decimal.
- Brands: trim brand_name. Categories: trim category_name.

Gold (Delta): category_revenue — join order_items -> products ->
categories (on category_id) and products -> brands (on brand_id); total
LineTotal as Revenue, total quantity as UnitsSold, and average list_price
as AvgListPrice per category_name and brand_name. Sort by Revenue
descending, top 5.

---

## BRD 4 — Bike-store product catalog cleanup (regex clean, price-band case)

Sources: `products.csv`, `categories.csv` in `s-landing`.

Bronze: copy raw.

Silver (Delta): trim product_name; strip the trailing model-year suffix
(e.g. " - 2016") off product_name with a regex replace; remove special
characters from product_name; cast list_price to decimal and model_year to
integer; keep only rows where list_price is greater than 0 and model_year
is between 2016 and 2018; bucket list_price into PriceBand: below 500 ->
'Budget', 500 to 1500 -> 'Mid', above 1500 -> 'Premium'; dedup on
product_id.

Gold (Delta): price_band_summary — join products to categories on
category_id; product count, average list_price, min list_price, and max
list_price per category_name and PriceBand. Sort by category_name
ascending.

---

## BRD 5 — Bike-store inventory risk (3-way join, sum, ascending top_n)

Sources: `stocks.csv`, `products.csv`, `stores.csv` in `s-landing`.

Bronze: copy raw.

Silver (Delta): drop rows with a missing store_id or product_id; cast
quantity to integer; flag rows where quantity equals 0 as OutOfStock
('Yes'/'No').

Gold (Delta): store_inventory_risk — join stocks -> products on
product_id, stocks -> stores on store_id; total quantity on hand and
distinct out-of-stock product count per store_name. Sort by total quantity
ascending, top 3 stores (the ones most at risk).

---

## BRD 6 — Olist customer geography (lower/upper, regex on zip, in/notlike filters)

Source: `olist_customers_dataset.csv` in `s-landing`.

Bronze: copy raw.

Silver (Delta): lowercase customer_city, uppercase customer_state, strip
any non-numeric characters from customer_zip_code_prefix with a regex
replace, keep only rows where customer_state is in the list
SP,RJ,MG,RS,PR and customer_city is not like '%unknown%', dedup on
customer_unique_id.

Gold (Delta): distinct customer count per customer_state and
customer_city. Sort by customer count descending, top 15.

---

## BRD 7 — Olist order timeline (timestamp cast, year/quarter derive, 3-way join)

Sources: `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`,
`olist_order_payments_dataset.csv` in `s-landing`.

Bronze: copy all three raw.

Silver (Delta):
- Orders: cast order_purchase_timestamp to timestamp; keep only rows where
  order_status equals 'delivered'; derive OrderYear (year) and
  OrderQuarter (quarter) from order_purchase_timestamp; dedup on order_id.
- Order items: cast price and freight_value to decimal; drop rows with a
  missing order_id.
- Payments: cast payment_value to decimal and payment_installments to
  integer; drop rows with a missing order_id.

Gold (Delta): revenue_by_quarter — join orders -> order_items -> payments
on order_id; total payment_value as Revenue, distinct order count, and
average payment_installments per OrderYear and OrderQuarter. Sort by
OrderYear then OrderQuarter ascending.

---

## BRD 8 — Olist freight economics (arithmetic ratio, like filter, first agg)

Sources: `olist_order_items_dataset.csv`, `olist_products_dataset.csv` in
`s-landing`.

Bronze: copy raw.

Silver (Delta):
- Order items: cast price and freight_value to decimal; drop rows where
  freight_value is less than 0; keep only rows where price is between 10
  and 5000; derive FreightRatio = freight_value / price; bucket
  FreightRatio into FreightTier: above 0.3 -> 'High', otherwise 'Normal'.
- Products: keep only rows where product_category_name is like 'moveis%'.

Gold (Delta): freight_by_category — join order_items to products on
product_id; average FreightRatio, first FreightTier, and count of order
items per product_category_name. Sort by average FreightRatio descending.

---

## BRD 9 — Copy-only staff/store roster (no transform, schedule)

Copy `staffs.csv` and `stores.csv` from the `s-landing` container
byte-for-byte into `bronze` — no transformations, no silver, no gold.
Verify each file exists and is not empty before copying. Run every day at
04:00 UTC.

---

## BRD 10 — Kitchen-sink bike-store BI mart (everything, 3 gold outputs)

Sources: `orders.csv`, `order_items.csv`, `products.csv`, `categories.csv`,
`brands.csv`, `stores.csv`, `staffs.csv`, `customers.csv` in `s-landing`.

Bronze: copy all eight raw.

Silver (Delta):
- Customers: trim and Proper Case first_name/last_name, concat into
  FullName, replace blank email with 'unknown@bikes.shop', dedup on
  customer_id.
- Orders: cast order_date to date; keep only rows where order_status is in
  the list 3,4; dedup on order_id.
- Order items: cast quantity to integer, list_price/discount to decimal;
  derive LineTotal = quantity * list_price * (1 - discount); drop rows
  with a missing product_id.
- Products, categories, brands, stores, staffs: trim name columns; cast
  list_price to decimal on products.

Gold (Delta), three outputs:
1. **monthly_revenue** — join orders -> order_items; total LineTotal as
   Revenue and order count per year and month (derived from order_date).
   Sort by year then month ascending.
2. **category_performance** — join order_items -> products -> categories
   -> brands; total LineTotal, units sold, and distinct order count per
   category_name and brand_name. Sort by Revenue descending, top 10.
3. **customer_value** — join orders -> customers; total spend and order
   count per customer FullName. After aggregation, bucket customers into
   ValueTier: total spend >= 5000 -> 'VIP', 1000 to 4999 -> 'Regular',
   otherwise 'Occasional'. Sort by total spend descending, top 25.

One master pipeline chains bronze -> silver -> gold, stop on failure.
Daily schedule at 03:00 UTC.

---

# Prescriptive/complex suite — 4 BRDs, exact ADF object names

BRDs 1-10 above are terse "business ask" style — they describe outcomes and
leave pipeline/dataflow/dataset naming to the compiler. These four are the
opposite: full production-style specs that name every pipeline, data flow,
dataset, and parameter explicitly, and claim pre-existing Azure resources
to reuse. All resource references below have been retargeted from the
originals to the resources that actually exist in this subscription:

- Resource Group: `Azure-data-lake` (was `RetailProject`)
- Data Factory: `azure-datalake-adf` (was `RetailADFFF`)
- Storage Account: `azurejaydeltalake` (was `retailproject3001`)
- Containers: `s-landing` / `s-bronze` / `s-silver` / `s-gold` (was
  `landing` / `bronze` / `silver` / `gold` — `s-bronze`/`s-silver`/`s-gold`
  don't exist yet; only `s-landing` does today, so these BRDs also test the
  compiler auto-creating the missing three under the `s-` prefix)

ADF object names inside each BRD (PL_*, DF_*, DS_*) are left exactly as
written — those are the BRD author's naming requirement, not
infrastructure to retarget.

---

## BRD 11 — Bike Store Analytics, ADF only, full 8-output gold layer

Build an End-to-End Bike Store Analytics Pipeline in Azure Data Factory ONLY

Do not create duplicate Azure resources.

### Source

Read the following CSV files from the **s-landing** container (read-only):

* customers.csv
* orders.csv
* order_items.csv
* products.csv
* categories.csv
* brands.csv
* stores.csv
* staffs.csv
* stocks.csv

### Storage

Containers:

* s-landing
* s-bronze
* s-silver
* s-gold

Formats:

* Landing → CSV
* Bronze → CSV
* Silver → Delta
* Gold → Delta

### Bronze

Copy every source dataset to **s-bronze**.

Requirements:

* No transformations
* Preserve schema
* Preserve column names
* Preserve nulls
* Preserve duplicates
* Preserve source file names

### Silver (ADF Mapping Data Flows)

Apply standard cleansing to every dataset:

* Remove duplicate business keys
* Remove null primary keys
* Trim string columns
* Convert required data types
* Replace required null values
* Rename columns to business-friendly names where appropriate

**Customers**
* Proper Case first_name and last_name
* Lowercase email
* Replace null phone with "Unknown"
* Create CustomerName
* Create CustomerAgeGroup
* Standardize city and state

**Orders**
* Convert all date columns to Timestamp
* Replace null shipped_date with order_date
* Create: OrderYear, OrderMonth, OrderQuarter, OrderWeekDay, IsWeekend

**Order Items**
* Convert numeric columns
* Remove quantity ≤ 0
* Create: LineTotal, DiscountAmount, NetSales, DiscountPercent

**Products**
* Replace null BrandID and CategoryID with 0
* Create: PriceBand, IsPremiumProduct, ProductStatus, ProductAge

**Categories**
* Trim names, uppercase category name

**Brands**
* Trim brand name, uppercase brand name

**Stores**
* Trim strings, uppercase state
* Create Region, StoreStatus

**Staffs**
* Convert Active to Boolean, keep active staff only
* Create StaffName, ManagerFlag

**Stocks**
* Convert quantity to Integer, replace negative quantity with 0
* Create StockStatus, InventoryLevel

Write every Silver dataset as Delta into **s-silver**.

### Gold

Create the following Delta outputs in **s-gold**.

**Monthly Sales** — Revenue, Orders, UnitsSold, AverageOrderValue; group by
Year, Month, Quarter.

**Sales by Category** — Revenue, UnitsSold, AverageSellingPrice,
ProductCount.

**Product Performance** — Revenue, UnitsSold, AverageSellingPrice,
AverageDiscount; sort by Revenue descending.

**Store Performance** — Revenue, Orders, DistinctCustomers,
AverageOrderValue.

**Brand Performance** — Revenue, UnitsSold, ProductCount.

**Customer Performance** — Revenue, TotalOrders, AverageOrderValue; derive
CustomerTier: Platinum, Gold, Silver, Bronze.

**Inventory Summary** — QuantityOnHand, NumberOfProducts; filter
QuantityOnHand > 0.

**Sales Trend** — Revenue, UnitsSold, Orders; group by Year, Month,
Quarter, WeekDay; sort chronologically.

### Pipelines

Create: `s_PL_Master`, `s_PL_Landing_To_Bronze`, `s_PL_Bronze_To_Silver`,
`s_PL_Silver_To_Gold`.

Execution: s-landing → s-bronze → s-silver → s-gold. Stop downstream
execution immediately if any stage fails.

### Data Flows

Silver: `s_DF_Customers_Silver`, `s_DF_Orders_Silver`,
`s_DF_OrderItems_Silver`, `s_DF_Products_Silver`, `s_DF_Categories_Silver`,
`s_DF_Brands_Silver`, `s_DF_Stores_Silver`, `s_DF_Staffs_Silver`,
`s_DF_Stocks_Silver`.

Gold: `s_DF_MonthlySales`, `s_DF_SalesByCategory`,
`s_DF_ProductPerformance`, `s_DF_StorePerformance`,
`s_DF_BrandPerformance`, `s_DF_CustomerPerformance`,
`s_DF_InventorySummary`, `s_DF_SalesTrend`.

### Datasets

Create reusable parameterized datasets: `s_DS_Landing_CSV`,
`s_DS_Bronze_CSV`, `s_DS_Silver_Delta`, `s_DS_Gold_Delta`.

Parameters: Container, Folder, Dataset, FileName.

### Validation

Before each dataset: verify file exists, verify file is not empty, log
failed validation, skip invalid dataset, continue processing remaining
valid datasets.

### Triggers

Manual Trigger, Daily Schedule Trigger.

### Deliverables

Automatically create: Parameterized Linked Service (reuse existing if
available), Parameterized Datasets, Mapping Data Flows, Pipelines,
Triggers, Pipeline Parameters.

The completed solution must implement a complete Medallion Architecture
using only Azure Data Factory and Azure Data Lake Storage:
**s-landing → s-bronze → s-silver → s-gold**

---

## BRD 12 — Bike Store Sales Medallion Pipeline, named pipelines/dataflows/6 gold outputs

Build an end-to-end Bike Store Sales Medallion Pipeline in Azure Data
Factory ONLY (no Databricks, Synapse, Microsoft Fabric, SQL Database,
Azure Functions, Logic Apps, or external compute).

### Existing Azure Resources

Resource Group: `Azure-data-lake`
Azure Data Factory: `azure-datalake-adf`
Azure Storage Account: `azurejaydeltalake`

Reuse the existing Azure resources. Do not create duplicate Azure
resources.

### Source

Read the following CSV files from the s-landing container:

* customers.csv
* orders.csv
* order_items.csv
* products.csv
* categories.csv
* brands.csv
* stores.csv
* staffs.csv
* stocks.csv

Treat the s-landing container as read-only. Do not modify, overwrite, or
delete any source files.

### Storage

Use the existing Azure Storage Account: `azurejaydeltalake`.

Containers: s-landing, s-bronze, s-silver, s-gold.

Reuse an existing Azure Data Factory Linked Service if available.

### Bronze

Copy every s-landing CSV file into the s-bronze container using Azure Data
Factory Copy Activities.

Perform no transformations. Preserve original schema, column names, null
values, and duplicate records.

Store each dataset as CSV using: `s-bronze/{dataset_name}/`

### Silver

Transform every Bronze dataset using Azure Data Factory Mapping Data
Flows.

**Customers** — remove duplicate customer_id records; remove rows where
customer_id is null; trim whitespace from all string columns; convert
first_name and last_name to Proper Case; convert email to lowercase.

**Orders** — remove duplicate order_id records; remove rows where order_id
is null; convert order_date, required_date and shipped_date to Timestamp;
replace null shipped_date with order_date.

**Order Items** — remove duplicate records; convert quantity to Integer,
list_price to Decimal(18,2), discount to Decimal(5,2); derives
`line_total = quantity * list_price * (1 - discount)`.

**Products** — remove duplicate product_id records; replace null
category_id with 0; replace null brand_id with 0; convert list_price to
Decimal(18,2).

**Categories** — remove duplicate category_id records; trim category_name.

**Brands** — remove duplicate brand_id records; trim brand_name.

**Stores** — remove duplicate store_id records; trim store_name, city,
state.

**Staffs** — remove duplicate staff_id records; convert active to Boolean.

**Stocks** — remove duplicate records; convert quantity to Integer.

Write every Silver dataset as Delta using:
`s-silver/customers/`, `s-silver/orders/`, `s-silver/order_items/`,
`s-silver/products/`, `s-silver/categories/`, `s-silver/brands/`,
`s-silver/stores/`, `s-silver/staffs/`, `s-silver/stocks/`

### Gold

**1. monthly_sales** — join Orders + Order Items on order_id; output Year,
Month, TotalSales; aggregate SUM(line_total). Write: `s-gold/monthly_sales/`

**2. sales_by_category** — join Order Items + Products + Categories on
product_id, category_id; output CategoryName, TotalSales; aggregate
SUM(line_total). Write: `s-gold/sales_by_category/`

**3. top_customers** — join Customers + Orders + Order Items on
customer_id, order_id; output customer_id, customer_name, TotalSales;
aggregate SUM(line_total); sort TotalSales descending; return top 10.
Write: `s-gold/top_customers/`

**4. store_sales** — join Stores + Orders + Order Items on store_id,
order_id; output StoreName, TotalSales, TotalOrders; aggregate
SUM(line_total), COUNT(order_id). Write: `s-gold/store_sales/`

**5. brand_performance** — join Brands + Products + Order Items on
brand_id, product_id; output BrandName, Revenue; aggregate SUM(line_total).
Write: `s-gold/brand_performance/`

**6. inventory_summary** — join Stocks + Products + Stores on product_id,
store_id; output StoreName, ProductName, QuantityOnHand; aggregate
SUM(quantity). Write: `s-gold/inventory_summary/`

### Orchestration

Create one Master Pipeline. Execution sequence: s-landing → s-bronze →
s-silver → s-gold.

* If Bronze fails, stop the pipeline.
* If Silver fails, Gold shall not execute.
* If Gold fails, mark the pipeline as Failed.

### Pipelines

Create: `PL_Master`, `PL_Landing_To_Bronze`, `PL_Bronze_To_Silver`,
`PL_Silver_To_Gold`.

### Data Flows

Silver: `DF_Customers_Silver`, `DF_Orders_Silver`, `DF_OrderItems_Silver`,
`DF_Products_Silver`, `DF_Categories_Silver`, `DF_Brands_Silver`,
`DF_Stores_Silver`, `DF_Staffs_Silver`, `DF_Stocks_Silver`.

Gold: `DF_Gold_MonthlySales`, `DF_Gold_SalesByCategory`,
`DF_Gold_TopCustomers`, `DF_Gold_StoreSales`, `DF_Gold_BrandPerformance`,
`DF_Gold_InventorySummary`.

### Datasets

Create reusable parameterized datasets for: Landing CSV, Bronze CSV,
Silver Delta, Gold CSV.

### Parameters

Parameterize: Container Name, Folder Path, Dataset Name, File Name. Avoid
hardcoded values wherever possible.

### Validation

Before processing every dataset: verify the source file exists, verify the
source file is not empty. If validation fails: stop processing that
dataset, prevent downstream execution.

### Triggers

Manual Trigger, Daily Schedule Trigger.

### Deliverables

Automatically create: Linked Services, Parameterized Datasets, Mapping
Data Flows, Pipelines, Pipeline Parameters, Triggers.

The completed solution shall execute successfully using Azure Data Factory
and Azure Storage following the Medallion Architecture:
s-landing → s-bronze → s-silver (Delta) → s-gold

---

## BRD 13 — Olist e-commerce, single monthly_sales gold output

Reuse the existing Azure resources. Do not create duplicate Azure
resources.

### Source

Read the following CSV files from the storage container `s-landing`.

Required datasets:

* olist_customers_dataset.csv
* olist_orders_dataset.csv
* olist_order_items_dataset.csv
* olist_products_dataset.csv
* olist_order_payments_dataset.csv

Treat the s-landing container as read-only. Do not modify or delete source
files.

### Storage

Use the existing Azure Data Lake Storage Gen2 account `azurejaydeltalake`.

Containers: s-landing, s-bronze, s-silver, s-gold.

Reuse an existing Azure Data Factory Linked Service if available.

### Bronze

Copy each s-landing CSV file byte-for-byte into the s-bronze container
using Azure Data Factory Copy Activities.

No transformations. Preserve original schema, column names, null values,
duplicate records.

Store Bronze data as CSV using the folder structure: `s-bronze/{dataset_name}/`

Parameterize the execution date.

### Silver

Transform the Bronze datasets using Azure Data Factory Mapping Data Flows.

**Customers** — remove duplicate customer_id records; remove rows where
customer_id is null; trim leading and trailing whitespace.

**Orders** — remove rows where order_id is null; convert
order_purchase_timestamp to Timestamp.

**Products** — replace null product_category_name with 'Unknown'.

**Payments** — convert payment_value to Decimal; remove rows where
payment_value is negative.

**Order Items** — remove duplicate records; convert numeric columns to
numeric datatypes.

Write all Silver datasets as Delta Lake tables using: `s-silver/{dataset_name}/`

### Gold

Create one business-ready dataset named `monthly_sales`.

Join Orders + Payments on order_id. Aggregate SUM(payment_value). Group by
Year(order_purchase_timestamp), Month(order_purchase_timestamp). Output
columns: Year, Month, TotalRevenue.

Write the output as CSV using: `s-gold/monthly_sales/`

### Orchestration

Create one Master Pipeline. Execution sequence: s-landing → s-bronze →
s-silver → s-gold.

If Bronze fails: stop pipeline. If Silver fails: stop Gold.

### Datasets

Create reusable parameterized datasets for: Landing CSV, Bronze CSV,
Silver Delta (inline — data flows only), Gold Delta.

### Parameters

Parameterize: Container, Folder, File Name, Dataset Name, Execution Date.
Avoid hardcoded paths.

### Validation

Before processing each dataset: verify the file exists, verify the file is
not empty. If validation fails: stop processing that dataset, prevent
downstream execution.

### Triggers

Manual Trigger, Daily Schedule Trigger.

### Deliverables

Automatically create: Linked Services, Parameterized Datasets, Mapping
Data Flows, Pipelines, Pipeline Parameters, Triggers.

---

## BRD 14 — Bike Store Analytics, condensed prescriptive form

Build an End-to-End Bike Store Analytics Pipeline in Azure Data Factory
ONLY.

### Resources

Reuse existing Azure resources:

* Resource Group: `Azure-data-lake`
* Azure Data Factory: `azure-datalake-adf`
* Storage Account: `azurejaydeltalake`

Do not create new Azure resources.

### Source

Read the following CSV files from the s-landing container (read-only):
customers, orders, order_items, products, categories, brands, stores,
staffs, stocks.

### Storage

s-landing → CSV, s-bronze → CSV, s-silver → Delta, s-gold → Delta.

### Bronze

Copy all source files without transformation. Preserve schema, column
names, nulls, and duplicates.

### Silver (Mapping Data Flows)

Apply deduplication, null-key removal, required type conversions,
trimming, null handling, and business-friendly column names.

* Customers: Proper Case names, lowercase email, default phone,
  CustomerName, CustomerAgeGroup.
* Orders: Timestamp conversion, default shipped_date, OrderYear,
  OrderMonth, OrderQuarter, OrderWeekDay.
* Order Items: numeric conversions, quantity > 0, LineTotal,
  DiscountAmount, NetSales.
* Products: default BrandID/CategoryID, PriceBand, IsPremiumProduct,
  ProductStatus.
* Categories: trim and uppercase category name.
* Brands: trim brand name.
* Stores: trim strings, uppercase state, Region.
* Staffs: Boolean active, active staff only, StaffName.
* Stocks: replace negative quantity with 0, StockStatus.

Write all Silver datasets as Delta.

### Gold

Create: Monthly Sales, Sales by Category, Product Performance, Store
Performance, Brand Performance, Customer Performance (CustomerTier:
Platinum, Gold, Silver, Bronze), Inventory Summary (QuantityOnHand > 0),
Sales Trend.

Include appropriate joins, aggregations, grouping, sorting, and derived
metrics such as Revenue, UnitsSold, Orders, AverageOrderValue,
AverageSellingPrice, ProductCount, DistinctCustomers, QuantityOnHand, and
NumberOfProducts.

Write all outputs as Delta.

### Pipelines

Create: `PL_Master`, `PL_Landing_To_Bronze`, `PL_Bronze_To_Silver`,
`PL_Silver_To_Gold`.

Execution: s-landing → s-bronze → s-silver → s-gold. Stop downstream
execution if any stage fails.

### Datasets

Create reusable parameterized datasets: Landing CSV, Bronze CSV, Silver
Delta, Gold Delta.

Parameters: Container, Folder, Dataset, FileName.

### Validation

Before each dataset: verify file exists, verify file is not empty, log
failures, skip invalid datasets, continue processing remaining valid
datasets.

### Triggers

Manual Trigger, Daily Schedule Trigger.
