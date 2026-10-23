---
name: adf-datasets-and-parameters
description: Sink/source dataset rules and parameter defaults.
---

# Datasets & parameters

- A data flow's sink must be a DIFFERENT dataset from its source — sinking to
  the source dataset overwrites the input and leaves the layer's container
  empty. Silver sink = its own `ds_silver_*` Delta dataset.
- Source datasets need a concrete file name; sink datasets don't (ADF generates
  file names into the folder).
- EVERY pipeline/dataset parameter MUST have a `defaultValue` — otherwise the
  run fails with "No value provided for Parameter". Keep the same parameter
  NAME across master → child → dataset or binding breaks. Parameterize only
  when the BRD truly needs per-run paths.
