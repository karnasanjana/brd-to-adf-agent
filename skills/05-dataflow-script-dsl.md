---
name: dataflow-script-dsl
description: Data-flow DSL rules - dedup, window, expression syntax.
---

# Data flow DSL (not Spark SQL, not T-SQL)

- Dedup = ONE aggregate: `group_by` = key columns, every non-key column in
  `columns` with op "first". Never sort + window(rowNumber) + filter.
- Window (ranking/running totals only): fill `partition_by` + `order_by` +
  `columns` (e.g. {"name": "row_num", "op": "row_number"}); leave `condition`
  empty. `partitionBy()`, `orderBy()`, `row_number()`, `ROW_NUMBER() OVER` do
  not exist in this DSL (DF-DSL-001).
- "TOP N" / "first N rows" = sort (desc on the ranking column) + ONE limit
  transform with `take`=N. Never a dummy partition key + window(rowNumber) +
  filter — that is 4 fragile steps for what ADF's native Limit transform does
  in one: `limit(skip, take)`.
- Transform kinds ONLY: select, cast, derived, filter, aggregate, sort, window,
  join, limit. ADF's exists/union/alterRow/keyGenerate/lookup cannot be
  expressed — restructure with joins/aggregates or leave the requirement out.
- Raw expressions (escape hatch only): `isNull(col)`/`!isNull(col)` never
  `col == null`; `iif(cond, a, b)` never `cond ? a : b`; bare column names,
  never `@{col}`; `stream@col` only inside join conditions; `{Braces For
  Spaced Names}`; computed groupBy key is `Year = year(ts)` never SQL `as`;
  string literals single-quoted, formats like 'yyyy-MM-dd HH:mm:ss'.
- Reference only columns that exist upstream (source column or an earlier
  transform's output) — else DF-EXPR-010 "Column unavailable".
