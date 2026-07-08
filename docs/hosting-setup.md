# Hosting the dashboard publicly

The local dashboard (`make demo`) talks directly to the live pipeline — real Kafka, real
Spark, real Airflow. None of that can run on a free static host, so the hosted version
is an explicitly-labeled **static snapshot**: a small cloud Postgres holds a periodic
copy of `live_order_metrics` and the 5 gold marts, and Streamlit Community Cloud serves
the same `dashboard/app.py` against that copy instead of localhost.

Two one-time signups, done once, by you — an agent has no browser and can't click
through either provider's UI.

## 1. Create a free cloud Postgres (Neon)

1. Go to https://neon.tech and sign up (GitHub login is the fastest option).
2. Create a new project — any name, any region close to you.
3. On the project's dashboard, find the **Connection string** panel. Copy the full URI —
   it looks like:
   ```
   postgresql://neondb_owner:AbC123xyz@ep-cool-name-12345.us-east-2.aws.neon.tech/neondb?sslmode=require
   ```
   That's your `CLOUD_DATABASE_URL`.

(Supabase works the same way if you'd rather use that — Project Settings → Database →
Connection string → URI. Either gives you a plain Postgres connection string, which is
all this needs.)

## 2. Point the sync script at it

1. Open `.env` and add the line, using your actual connection string from step 1:
   ```
   CLOUD_DATABASE_URL=postgresql://neondb_owner:AbC123xyz@ep-cool-name-12345.us-east-2.aws.neon.tech/neondb?sslmode=require
   ```
2. With the local stack up and some data in it (`make up`, `make produce` for a bit,
   let the `batch_quality_marts` DAG run at least once), run:
   ```
   python scripts/sync_to_cloud_demo.py
   ```
   This creates the schema in the cloud DB (same DDL the local stack uses) and copies
   the last 24h of `live_order_metrics` plus all 5 marts tables into it. Re-run it
   any time you want to refresh the public snapshot — there's no scheduled job for this
   on purpose, it's a manual "publish" step.

## 3. Deploy the dashboard on Streamlit Community Cloud

1. Go to https://share.streamlit.io and sign in with GitHub.
2. Click **New app**, pick this repo, branch `main`, and set the main file path to
   `dashboard/app.py`.
3. Before deploying (or right after, from the app's **Settings → Secrets**), paste in a
   TOML block with the same connection details, but broken into the individual fields
   the dashboard expects (not the single URI this time — `dashboard/app.py` reads these
   as separate keys):
   ```toml
   POSTGRES_HOST_EXTERNAL = "ep-cool-name-12345.us-east-2.aws.neon.tech"
   POSTGRES_PORT_EXTERNAL = "5432"
   POSTGRES_DB = "neondb"
   POSTGRES_USER = "neondb_owner"
   POSTGRES_PASSWORD = "AbC123xyz"
   ```
   (Pull these values out of the same connection string from step 1 — host is everything
   between `@` and the first `/`, db name is after the final `/` before `?`.)
4. Deploy. First build takes a couple of minutes.

## 4. Verify

- The hosted URL loads and shows the "static demo snapshot" caption under the title —
  that caption only appears when the dashboard is reading `st.secrets` instead of your
  local `.env`, so seeing it confirms the cloud connection is the one actually being used.
- Data in both tabs matches what `python scripts/sync_to_cloud_demo.py` last printed.
- The local dashboard (`make demo`) still works exactly as before — hosting this didn't
  change anything about the local path, `st.secrets` access is wrapped in a try/except
  specifically so a machine with no `secrets.toml` at all (every local run) falls
  straight through to the existing `.env` behavior.

## Keeping it fresh

The hosted snapshot only updates when you run `python scripts/sync_to_cloud_demo.py`
again. Do that occasionally if you want the public demo to look current — there's no
requirement to run it continuously, and no local services need to be up except for that
one moment (Postgres has to be reachable to read from; the cloud DB has to be reachable
to write to).
