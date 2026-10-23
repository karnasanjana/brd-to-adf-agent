---
name: adf-activities-and-orchestration
description: Modelling BRD asks that aren't real activities.
---

# BRD asks that aren't buildable activities

- "Validate file exists / handle missing or empty files" → plain Copy; ADF
  surfaces those errors at run time.
- "Archive landing after gold" → a Copy to the archive container (never Delete).
- "Logging table / run metrics" → omit; that is Azure Monitor configuration,
  not a pipeline activity.
