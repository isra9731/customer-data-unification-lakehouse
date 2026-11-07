# Architecture

## Current foundation

```mermaid
flowchart TD
    subgraph Sources
        C[customers.csv]
        T[transactions.csv]
        P[contacts.csv]
    end
    subgraph Fabric Lakehouse
        B[Bronze: raw source tables]
        S[Silver: standardised and validated tables]
        M[Silver: entity match candidates]
        subgraph Gold
            D[Customer dimension]
            F[Monthly spend fact]
            X[Entity cross-reference]
            E[Unified entity master]
        end
    end
    C --> B
    T --> B
    P --> B
    B --> S
    S --> M
    S --> D
    S --> F
    M --> X --> E
```

## Planned extensions

- Watermark-controlled incremental ingestion and Delta MERGE.
- Quarantine tables and reusable data quality rules.
- SCD Type 2 customer dimension.
- RFM, cohort, retention and customer lifetime value analytics.
- Entity match confidence bands and manual review workflow.
- Pipeline audit and run-level operational metrics.
