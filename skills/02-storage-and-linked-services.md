---
name: adf-storage-and-linked-services
description: Blob vs ADLS Gen2 endpoint rule.
---

# Storage endpoints

- ADLS Gen2 = HNS ON = `.dfs.core.windows.net` (AzureBlobFS). Plain Blob = HNS
  OFF = `.blob.core.windows.net` (AzureBlobStorage). NOT interchangeable — a
  non-HNS account fails on the .dfs endpoint. Unsure → plain Blob works for
  medallion copy + data flows.
- Declare one `ls_` per storage account with its account URL; the builder picks
  the concrete linked-service type. HTTP source: HttpServer, Anonymous auth.
