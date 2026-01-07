#!/usr/bin/env python3
"""
scrape.py
Scrapes TeamRankings ATS trends for multiple leagues and stores DAILY snapshots in Supabase/Postgres.

Leagues supported:
- nba: https://www.teamrankings.com/nba/trends/ats_trends/
- nfl: https://www.teamrankings.com/nfl/trends/ats_trends/
- ncb: https://www.teamrankings.com/ncb/trends/ats_trends/
- ncf: https://www.teamrankings.com/ncf/trends/ats_trends/

DB design:
- scrape_runs: one record per league run (metadata)
- ats_trends: one row per (league, team, scrape_date) with row_json snapshot
"""

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Dict, List

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.dialects.postgresql import JSONB


LEAGUE_URLS: Dict[str, str] = {
    "nba": "https://www.teamrankings.com/nba/trends/ats_trends/",
    "nfl": "https://www.teamrankings.com/nfl/trends/ats_trends/",
    "ncb": "https://www.teamrankings.com/ncb/trends/ats_trends/",
    "ncf": "https://www.teamrankings.com/ncf/trends/ats_trends/",
}

SOURCE_NAME = "teamrankings_ats_trends"

HEADERS = {"User-Agent": "Mozilla/5.0"}

REQUEST_TIMEOUT_SECS = 30


def fetch_soup(url: str) -> BeautifulSoup:
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECS)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def extract_table(soup: BeautifulSoup) -> List[Dict[str, str]]:
    table = soup.find("table")
    if table is None:
        raise RuntimeError("No <table> found on the page.")

    rows = table.find_all("tr")
    if not rows:
        return []

    headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]
    if not headers:
        # some tables might use td for header row (rare)
        headers = [td.get_text(strip=True) for td in rows[0].find_all("td")]

    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        tds = row.find_all("td")
        if not tds:
            continue
        values = [td.get_text(strip=True) for td in tds]

        # Guard against mismatched columns
        if len(values) != len(headers):
            # If there's an extra blank col etc, best effort zip up to min length
            n = min(len(values), len(headers))
            rec = dict(zip(headers[:n], values[:n]))
        else:
            rec = dict(zip(headers, values))

        data.append(rec)

    return data


def stable_row_hash(row: Dict[str, str]) -> str:
    """
    Stable hash of the row contents to record what changed within a day.
    (Daily uniqueness is enforced separately via (league, team, scrape_date).)
    """
    payload = json.dumps(row, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def infer_team(row: Dict[str, str]) -> str:
    """
    Team column header can vary, but for TeamRankings it's usually "Team".
    Fall back to first column value if needed.
    """
    for key in ("Team", "TEAM", "team"):
        if key in row and row[key]:
            return row[key].strip()

    # fallback: first value
    if row:
        return next(iter(row.values())).strip()

    return "UNKNOWN"


def ensure_schema(conn) -> None:
    conn.execute(text("""
        create table if not exists scrape_runs (
          run_id      bigserial primary key,
          source      text not null,
          league      text not null,
          url         text not null,
          scraped_at  timestamptz not null default now()
        );
    """))

    conn.execute(text("""
        create table if not exists ats_trends (
          id          bigserial primary key,
          run_id      bigint not null references scrape_runs(run_id) on delete cascade,
          scraped_at  timestamptz not null,
          scrape_date date generated always as ((scraped_at at time zone 'UTC')::date) stored,
          league      text not null,
          team        text not null,
          row_json    jsonb not null,
          row_hash    text not null,
          unique (league, team, scrape_date)
        );
    """))

    conn.execute(text("""
        create index if not exists idx_ats_trends_league_team_time
          on ats_trends (league, team, scraped_at desc);
    """))

    # Handy flat view for Sheets / REST (casts where reasonable)
    conn.execute(text("""
        create or replace view flat_ats_trends_v as
        select
          scrape_date,
          league,
          team,
          row_json->>'ATS Record' as ats_record,
          row_json->>'Cover %' as cover_pct,
          nullif(row_json->>'MOV','')::numeric as mov,
          nullif(replace(row_json->>'ATS +/-', '+', ''),'')::numeric as ats_plus_minus
        from ats_trends;
    """))


def main() -> None:
    load_dotenv()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set. Put it in your .env or environment variables.")

    # Tip: for Supabase pooler, your DATABASE_URL should usually include sslmode=require
    # Example:
    # postgresql://postgres:PASS@aws-0-xxx.pooler.supabase.com:6543/postgres?sslmode=require&connect_timeout=10

    engine = create_engine(db_url, future=True)

    scraped_at = datetime.now(timezone.utc)

    insert_run_stmt = text("""
        insert into scrape_runs (source, league, url, scraped_at)
        values (:source, :league, :url, :scraped_at)
        returning run_id
    """)

    insert_trend_stmt = text("""
        insert into ats_trends
          (run_id, scraped_at, league, team, row_json, row_hash)
        values
          (:run_id, :scraped_at, :league, :team, :row_json, :row_hash)
        on conflict (league, team, scrape_date)
        do update set
          run_id     = excluded.run_id,
          scraped_at = excluded.scraped_at,
          row_json   = excluded.row_json,
          row_hash   = excluded.row_hash
    """).bindparams(bindparam("row_json", type_=JSONB))

    total_inserted_or_updated = 0

    with engine.begin() as conn:
        ensure_schema(conn)

        for league, url in LEAGUE_URLS.items():
            print(f"\n=== {league.upper()} ===")
            print(f"Fetching: {url}")

            soup = fetch_soup(url)
            rows = extract_table(soup)
            print(f"Rows parsed: {len(rows)}")

            run_id = conn.execute(
                insert_run_stmt,
                {"source": SOURCE_NAME, "league": league, "url": url, "scraped_at": scraped_at},
            ).scalar_one()

            league_count = 0
            for row in rows:
                team = infer_team(row)
                row_hash = stable_row_hash(row)

                conn.execute(
                    insert_trend_stmt,
                    {
                        "run_id": run_id,
                        "scraped_at": scraped_at,
                        "league": league,
                        "team": team,
                        "row_json": row,
                        "row_hash": row_hash,
                    },
                )
                league_count += 1

            total_inserted_or_updated += league_count
            print(f"Upserted snapshots (team/day): {league_count} (run_id={run_id})")

    print(f"\nDone. Total upserts across leagues: {total_inserted_or_updated}")
    print(f"scraped_at (UTC): {scraped_at.isoformat()}")


if __name__ == "__main__":
    main()
