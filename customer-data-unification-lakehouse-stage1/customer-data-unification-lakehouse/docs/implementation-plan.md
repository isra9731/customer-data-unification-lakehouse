# Implementation plan

| Stage | Scope | Status |
|---|---|---|
| 1 | Repository structure and notebook foundation | In progress |
| 2 | Incremental ingestion and Delta MERGE | Planned |
| 3 | Data quality framework and quarantine | Planned |
| 4 | SCD Type 2 customer history | Planned |
| 5 | Entity matching improvements and review queue | Planned |
| 6 | Customer analytics: RFM, cohorts, retention, CLV | Planned |
| 7 | Pipeline audit, automated tests and optimisation | Planned |

## Engineering rules

- Keep source data unchanged in Bronze.
- Make transformations deterministic and repeatable.
- Preserve source identifiers and ingestion metadata.
- Separate invalid records from trusted analytical outputs.
- Do not claim a feature is complete until it has been executed and validated in Fabric.
- Use synthetic or appropriately licensed datasets only.
