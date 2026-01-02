# Phlo Examples

This repository contains example Phlo projects you can use as references or starting points.
Each example is a standalone project with its own `phlo.yaml`, `workflows/`, and
`workflows/transforms/dbt` dbt project.

## Examples

- `nightscout/`: Nightscout glucose data ingestion, quality, and dbt transforms.
- `github/`: GitHub API ingestion with merge strategies and dbt marts.
- `pokemon/`: PokeAPI ingestion with silver/gold dbt models.
- `analyst-duckdb-demo/`: Lightweight demo using DuckDB for local analysis.

## Common Structure

```
example/
├── .phlo/                       # Local infra config
├── workflows/                   # Ingestion + quality + transforms
│   ├── ingestion/
│   ├── schemas/
│   └── transforms/dbt/
├── tests/
├── phlo.yaml
└── pyproject.toml
```

## Using an Example

1. Copy an example into a new directory.
2. Follow the README inside the example for setup and run commands.
