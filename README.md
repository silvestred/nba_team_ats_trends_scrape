# 📊 Multi-League Sports Analytics Data Pipeline

A production-style **data engineering and analytics pipeline** that ingests daily **Against-The-Spread (ATS)** trends across multiple sports leagues, stores **historical snapshots**, and exposes the data via a **REST API** for live consumption in **Google Sheets dashboards**.

Built to demonstrate **real-world data engineering patterns**: snapshotting, idempotent ingestion, schema design, API access, and end-user analytics.

---

## 🔍 What This Project Solves

Sports trend data changes daily, but most dashboards only show “today.”

This project enables:
- Historical trend analysis
- “What did this look like X days ago?”
- Multi-league comparison
- Analytics-ready querying for non-technical users

---

## 🧠 System Architecture

Scraper → Database → Views → API → Google Sheets

1. **Python Scraper**
   - Scrapes ATS trends by league
   - Normalizes rows and computes hashes
   - Writes immutable historical records

2. **PostgreSQL (Supabase)**
   - `ats_trends` table stores all historical snapshots
   - `scrape_runs` tracks each ingestion run
   - SQL views expose:
     - latest snapshots
     - flat historical data
     - typed numeric fields for analytics

3. **REST API (Supabase PostgREST)**
   - Google Sheets queries data via HTTP
   - Supports filters by league, date, and team

4. **Google Sheets**
   - Acts as a BI/analytics front-end
   - Supports:
     - latest snapshots
     - historical views
     - dynamic “X days ago” queries

5. **Automation**
   - GitHub Actions runs the scraper on a schedule
   - Google Apps Script refreshes Sheets automatically



---

## 🚀 Key Features

- Daily ATS trend ingestion
- Multi-league support:
  - NBA
  - NFL
  - NCB (College Basketball)
  - NCF (College Football)
- Time-series snapshot storage
- Idempotent inserts (no duplicate rows)
- Analytics-ready PostgreSQL views
- REST API access via Supabase
- Dynamic Google Sheets dashboards
- User-controlled historical queries (X days ago)

---

## 🗄️ Data Modeling

### Core Table: `ats_trends`

Stores **immutable daily snapshots** of ATS data.

```sql
create table ats_trends (
  id bigserial primary key,
  league text not null,
  team text not null,
  row_json jsonb not null,
  scraped_at timestamptz not null,
  scrape_date date not null,
  row_hash text not null,
  unique (league, team, row_hash)
);
```
## Why This Model Works

Snapshot-based (no overwrites)

Supports historical analysis

Prevents duplicate inserts

Preserves raw source data

Allows schema evolution without breaking consumers

## 📈 Analytics Views

`flat_ats_trends_v`

Typed, analytics-ready view used for historical queries.
```sql
create or replace view flat_ats_trends_v as
select
  scrape_date,
  league,
  team,
  row_json->>'ATS Record' as ats_record,
  row_json->>'Cover %' as cover_pct,
  nullif(row_json->>'MOV','')::numeric as mov,
  nullif(replace(row_json->>'ATS +/-','+',''),'')::numeric as ats_plus_minus
from ats_trends;
```
`latest_ats_trends_v`

Returns the most recent snapshot per league + team.

```sql
create or replace view latest_ats_trends_v as
select distinct on (league, team)
  league,
  team,
  row_json->>'ATS Record' as ats_record,
  row_json->>'Cover %' as cover_pct,
  nullif(row_json->>'MOV','')::numeric as mov,
  nullif(replace(row_json->>'ATS +/-','+',''),'')::numeric as ats_plus_minus,
  scrape_date
from ats_trends
where scrape_date = current_date
order by league, team, scraped_at desc;
```
## 🐍 Data Ingestion (Python)

Scrapes ATS trend tables

Normalizes and hashes row data

Inserts into PostgreSQL using SQLAlchemy

Safe to run multiple times per day

Designed for automation

Engineering Concerns Addressed

Idempotency

Time-series correctness

Data normalization

Schema stability

## 📊 Google Sheets Dashboards

Google Sheets acts as the analytics frontend.

Live Dashboards

One API call fetches all leagues

Data automatically split into tabs:

  NBA ATS

  NFL ATS

  NCB ATS

  NCF ATS

Historical Querying

  Users can:

  Select X days ago

  Filter by league
  
  Filter by team

Sheets dynamically query the REST API using Apps Script.

## 🔄 Automation

Scraper scheduled daily

Google Sheets auto-refresh via time-based triggers

No manual intervention required

## 🛠️ Tech Stack

Python

PostgreSQL

Supabase

SQL / PostgREST

Google Apps Script

Google Sheets

## 🔐 Security

Read-only API access for dashboards

Row-Level Security enforced in Supabase

API keys stored securely (not hardcoded)

No write access from Sheets

## 📌 Why This Project Matters

This project demonstrates:

Real-world data engineering design

Analytics-first modeling

API-driven dashboards

User-friendly data access

End-to-end ownership of a data system

It reflects the type of systems built in modern data teams, not toy examples.

## 👤 Author

Built as a hands-on data engineering portfolio project focused on:

Time-series analytics

Data pipelines

Backend + analytics integration

Practical problem-solving
