# Exporting the marts to BigQuery

The batch layer already lands 5 gold marts in Postgres (`airflow/dags/batch_quality_marts.py`).
`export_to_bigquery.py` is a separate, independent DAG that copies those same marts out to GCS
(Parquet) and then loads them into BigQuery, using Airflow's official Google provider operators
(`PostgresToGCSOperator` → `GCSToBigQueryOperator`) instead of raw `google-cloud-bigquery` calls.

This needs your own GCP project — there's no way around clicking through the GCP console
and holding your own credentials, so the one-time setup below has to be run by hand.

## One-time GCP setup (run these yourself, in order)

```bash
# 1. Pick/create a project (skip if you already have one you want to use)
gcloud projects create ecommerce-pipeline-demo --name="Ecommerce Pipeline Demo"
gcloud config set project ecommerce-pipeline-demo

# 2. Enable the two APIs this needs
gcloud services enable bigquery.googleapis.com storage.googleapis.com

# 3. Create the GCS bucket (bucket names are globally unique -- pick your own)
gcloud storage buckets create gs://YOUR-UNIQUE-BUCKET-NAME --location=US

# 4. Create the BigQuery dataset
bq mk --dataset --location=US ecommerce-pipeline-demo:ecommerce_marts

# 5. Create a service account for the DAG
gcloud iam service-accounts create bq-export-dag \
  --display-name="Airflow BigQuery export DAG"

# 6. Grant it the three roles it needs (bucket read/write + BQ table write + BQ job submit --
# bigquery.dataEditor alone is not enough, it covers table data/schema but not actually
# submitting a load job; confirmed by hitting a real 403 without jobUser)
gcloud projects add-iam-policy-binding ecommerce-pipeline-demo \
  --member="serviceAccount:bq-export-dag@ecommerce-pipeline-demo.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding ecommerce-pipeline-demo \
  --member="serviceAccount:bq-export-dag@ecommerce-pipeline-demo.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding ecommerce-pipeline-demo \
  --member="serviceAccount:bq-export-dag@ecommerce-pipeline-demo.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# 7. Download its key -- this is the file the DAG authenticates with
gcloud iam service-accounts keys create gcp-key.json \
  --iam-account=bq-export-dag@ecommerce-pipeline-demo.iam.gserviceaccount.com
```

Then, on your end:

1. Move the downloaded `gcp-key.json` into `secrets/gcp-key.json` at the repo root (this
   directory is gitignored — the key never gets committed).
2. In `.env`, set `GCP_PROJECT_ID=ecommerce-pipeline-demo`,
   `GCP_GCS_BUCKET=YOUR-UNIQUE-BUCKET-NAME`, `GCP_BQ_DATASET=ecommerce_marts` (matching
   whatever you actually used above).
3. `docker compose up -d --build` (picks up the new `secrets/` mount and env vars), then
   trigger the DAG once from the Airflow UI (`http://localhost:8080` → `export_to_bigquery`
   → trigger) or `docker compose exec airflow-scheduler airflow dags trigger export_to_bigquery`.

## Verify

Trigger the DAG manually, confirm Parquet lands in the GCS bucket, confirm rows land in
BigQuery, spot-check row counts match Postgres.

## Notes from running this against a real project

Three things came up wiring this against an actual GCP project that a sandboxed pass
wouldn't catch:

1. **`PostgresToGCSOperator`'s Parquet path has a real type-inference bug.** It stringifies
   Postgres `DATE` values (via `convert_type()`) but separately infers the target column as
   pyarrow's native `date32()` for a source `DATE` column (via `_convert_parquet_schema()`)
   — two code paths in `apache-airflow-providers-google==22.2.1` that disagree, so every
   mart failed with `ArrowTypeError: object of type <class 'str'> cannot be converted to
   int`. Fixed by casting `event_date::text` in the DAG's own SQL, which makes the
   operator's type inference agree with what it already produces.
2. **`PostgresToGCSOperator` also needs a `google_cloud_default` Connection row to exist**,
   not just `GOOGLE_APPLICATION_CREDENTIALS` set — its GCS upload step calls
   `get_connection("google_cloud_default")` before ADC fallback even gets a chance, and
   Airflow raises `AirflowNotFoundException` if no Connection with that id exists at all.
   Fixed with an `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=google-cloud-platform://` env var (same
   convention as `AIRFLOW_CONN_POSTGRES_DEFAULT`) — the connection exists to satisfy the
   lookup, `GOOGLE_APPLICATION_CREDENTIALS` still does the actual authenticating.
3. **The service account needs a third IAM role**, `roles/bigquery.jobUser`, on top of the
   two roles above — that's why the setup steps above already include it (added after
   `GCSToBigQueryOperator` failed with a real `403 Forbidden` without it).

After all three fixes: the DAG ran end to end, Parquet landed in the GCS bucket, all 5
marts landed in BigQuery with row counts matching Postgres exactly, and the fix held up
from a clean container restart using only the docker-compose env vars — not something that
only worked because of a manual one-off CLI command.
