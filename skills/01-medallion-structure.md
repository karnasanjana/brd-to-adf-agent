---
name: adf-medallion-structure
description: Bronze/Silver/Gold layout rules.
---

# Medallion layout

- Bronze = Copy activity, byte-for-byte into bronze container. No transforms, no data flow.
- Silver = Data Flow `layer="silver"`: cast/trim/dedup/null-drop. Reads the bronze
  dataset, writes a SEPARATE `ds_silver_*` Delta dataset (container `silver`) —
  never back onto the dataset it reads.
- Gold = Data Flow `layer="gold"`: aggregates. Reads silver Delta only (never
  bronze), writes CSV/Parquet to gold.
- One child pipeline per layer + one master of ExecutePipeline activities chaining
  Bronze → Silver → Gold.
