# nyc_taxis_dab

NYC taxi ETL on Databricks, managed as a [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/index.html).

## Layout

```
├── databricks.yml              # bundle config (dev target)
├── resources/
│   ├── nyc_taxis_etl.pipeline.yml   # Lakeflow pipeline
│   └── sample_job.job.yml           # sample orchestration job
├── src/
│   ├── nyc_taxis_dab/          # shared Python package (wheel job tasks)
│   └── nyc_taxis_etl/          # pipeline transformations (.py)
├── tests/
└── fixtures/
```

Sample data source: `samples.nyctaxi.trips` (built-in Databricks sample dataset).

## Local setup

```bash
# install uv: https://docs.astral.sh/uv/getting-started/installation/
uv sync --dev
```

## Deploy (dev)

```bash
databricks bundle validate
databricks bundle deploy
```

In development mode, resources are prefixed with `[dev <your_username>]` and job schedules are paused.

## Run

```bash
# run the sample job
databricks bundle run sample_job

# or run the pipeline directly
databricks bundle run nyc_taxis_etl
```

## Tests

```bash
uv run pytest
```
