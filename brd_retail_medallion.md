Build an end-to-end Retail Sales Medallion Pipeline in Azure Data Factory ONLY (no Databricks, Synapse, Microsoft Fabric, Azure Functions, SQL Database, or external compute).

EXISTING AZURE RESOURCES:

Resource Group:
RetailProject

Azure Data Factory:
RetailADFFF

Azure Data Lake Storage Gen2 Account:
retailproject3001

Reuse the existing Azure resources. Do not create duplicate Azure resources.

SOURCE:

Read the following CSV files from the storage container 'landing'.

Required datasets:

- olist_customers_dataset.csv
- olist_orders_dataset.csv
- olist_order_items_dataset.csv
- olist_products_dataset.csv
- olist_order_payments_dataset.csv

Treat the Landing container as read-only.

Do not modify or delete source files.

STORAGE:

Use the existing Azure Data Lake Storage Gen2 account 'retailproject3001'.

Containers:

- landing
- bronze
- silver
- gold

Reuse an existing Azure Data Factory Linked Service if available.

BRONZE:

Copy each Landing CSV file byte-for-byte into the Bronze container using Azure Data Factory Copy Activities.

No transformations.

Preserve:

- original schema
- column names
- null values
- duplicate records

Store Bronze data as CSV using the folder structure:

bronze/{dataset_name}/

Parameterize the execution date.

SILVER:

Transform the Bronze datasets using Azure Data Factory Mapping Data Flows.

Customers

- Remove duplicate customer_id records.
- Remove rows where customer_id is null.
- Trim leading and trailing whitespace.

Orders

- Remove rows where order_id is null.
- Convert order_purchase_timestamp to Timestamp.

Products

- Replace null product_category_name with 'Unknown'.

Payments

- Convert payment_value to Decimal.
- Remove rows where payment_value is negative.

Order Items

- Remove duplicate records.
- Convert numeric columns to numeric datatypes.

Write all Silver datasets as Delta Lake tables using:

silver/{dataset_name}/

GOLD:

Create one business-ready dataset named monthly_sales.

Join:

Orders
+
Payments

Join Key:

order_id

Aggregate:

SUM(payment_value)

Group By:

Year(order_purchase_timestamp)

Month(order_purchase_timestamp)

Output Columns:

- Year
- Month
- TotalRevenue

Write the output as CSV using:

gold/monthly_sales/

ORCHESTRATION:

Create one Master Pipeline.

Execution sequence:

Landing

↓

Bronze

↓

Silver

↓

Gold

If Bronze fails:

Stop pipeline.

If Silver fails:

Stop Gold.

PIPELINES:

Create:

PL_Retail_Master

PL_Landing_To_Bronze

PL_Bronze_To_Silver

PL_Silver_To_Gold

DATA FLOWS:

Create:

DF_Customers_Silver

DF_Orders_Silver

DF_Products_Silver

DF_Payments_Silver

DF_OrderItems_Silver

DF_Gold_MonthlySales

DATASETS:

Create reusable parameterized datasets for:

Landing CSV

Bronze CSV

Silver Delta (inline — data flows only)

Gold CSV

PARAMETERS:

Parameterize:

- Container
- Folder
- File Name
- Dataset Name
- Execution Date

Avoid hardcoded paths.

VALIDATION:

Before processing each dataset:

- Verify the file exists.
- Verify the file is not empty.

If validation fails:

- Stop processing that dataset.
- Prevent downstream execution.

TRIGGERS:

Provide:

- Manual Trigger

- Daily Schedule Trigger

DELIVERABLES:

Automatically create:

- Linked Services
- Parameterized Datasets
- Mapping Data Flows
- Pipelines
- Pipeline Parameters
- Triggers

The completed solution shall execute successfully using Azure Data Factory and Azure Blob Storage following the Medallion Architecture (Landing → Bronze → Silver → Gold).
