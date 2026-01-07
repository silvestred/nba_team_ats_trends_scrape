#!/usr/bin/env python3
import os
import json
import hashlib
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import bindparam

URL = "https://www.teamrankings.com/nba/trends/ats_trends/"
HEADERS = {"User-Agent": "Mozilla/5.0"}
SOURCE_NAME = "teamrankings_nba_ats_trends"


def fetch_soup(url: str) -> BeautifulSoup:
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def extract_table_rows(soup: BeautifulSoup) -> list[dict]:
    """
    Extracts the first table on the page into list[dict].
    If TeamRankings changes markup, you may need to refine the selector.
    """
    table = soup.select_one("table")
    if not table:
        raise RuntimeError("No table found on the page.")

    # Prefer thead headers; fall back to first row th cells
    header_cells = table.select("thead th")
    if not header_cells:
        header_cells = table.select("tr th")

    headers = [h.get_text(strip=True) for h in header_cells]
    headers = [h if h else f"col_{i}" for i, h in enumerate(headers)]

    body_rows = table.select("tbody tr")
    if not body_rows:
        # fallback: all rows after the first
        body_rows = table.select("tr")[1:]

    data: list[dict] = []
    for tr in body_rows:
        tds = tr.select("td")
        if not tds:
            continue
        values = [td.get_text(" ", strip=True) for td in tds]
        row = dict(zip(headers, values))
        data.append(row)

    return data


def stable_row_hash(row: dict) -> str:
    payload = json.dumps(row, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ensure_schema(conn) -> None:
    """
    Creates tables/indexes if they don't exist.
    NOTE: This requires the DB user to have CREATE privilege on schema public.
    If your scraper_user cannot CREATE, run these DDL statements once as postgres.
    """
    conn.execute(text("""
        create table if not exists scrape_runs (
          run_id        bigserial primary key,
          source        text not null,
          url           text not null,
          scraped_at    timestamptz not null default now()
        );
    """))

    conn.execute(text("""
        create table if not exists nba_ats_trends (
          id            bigserial primary key,
          run_id        bigint not null references scrape_runs(run_id) on delete cascade,
          scraped_at    timestamptz not null,
          team          text not null,
          row_json      jsonb not null,
          row_hash      text not null
        );
    """))

    conn.execute(text("""
        create index if not exists idx_nba_ats_trends_team_time
          on nba_ats_trends (team, scraped_at desc);
    """))

    conn.execute(text("""
        create unique index if not exists ux_nba_ats_trends_dedupe
          on nba_ats_trends (team, row_hash);
    """))


def main():
    load_dotenv()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL is not set. Add it to your .env or export it in your shell.\n"
            "Example:\n"
            "DATABASE_URL=postgresql://scraper_user:scraper_pass@localhost:5432/scraper_db"
        )

    engine = create_engine(db_url, future=True)

    soup = fetch_soup(URL)
    rows = extract_table_rows(soup)
    scraped_at = datetime.now(timezone.utc)

    # Prepare statement for inserting rows (bind row_json as JSONB)
    insert_trend_stmt = text("""
        insert into nba_ats_trends (run_id, scraped_at, team, row_json, row_hash)
        values (:run_id, :scraped_at, :team, :row_json, :row_hash)
        on conflict (team, row_hash) do nothing
    """).bindparams(bindparam("row_json", type_=JSONB))

    with engine.begin() as conn:
        # Create schema if needed
        ensure_schema(conn)

        # Insert scrape run
        run_id = conn.execute(
            text("""
                insert into scrape_runs (source, url, scraped_at)
                values (:source, :url, :scraped_at)
                returning run_id
            """),
            {"source": SOURCE_NAME, "url": URL, "scraped_at": scraped_at},
        ).scalar_one()

        inserted = 0
        for row in rows:
            # Try to identify the team field robustly
            team = (
                row.get("Team")
                or row.get("TEAM")
                or row.get("team")
                or next(iter(row.values()), None)
                or "UNKNOWN"
            ).strip()

            row_hash = stable_row_hash(row)

            res = conn.execute(
                insert_trend_stmt,
                {
                    "run_id": run_id,
                    "scraped_at": scraped_at,
                    "team": team,
                    "row_json": row,      # dict -> JSONB
                    "row_hash": row_hash,
                },
            )
            inserted += res.rowcount

    print(
        f"OK ✅ run_id={run_id} scraped_at={scraped_at.isoformat()} "
        f"rows_seen={len(rows)} rows_inserted={inserted}"
    )


if __name__ == "__main__":
    main()
