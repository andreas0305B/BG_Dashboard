#!/usr/bin/env python3
"""
find_match_ids.py

Standalone script to find missing DailyGammon match_id values and write them into Neon (Postgres).
- If --season not provided it uses the highest saison_nummer in groups.
- If --league provided, only that group is processed; otherwise all groups in the season are processed.
- Suitable to run from cron (e.g. 2x daily) or to be invoked from Streamlit via subprocess.

Requirements:
  pip install requests beautifulsoup4 python-dotenv psycopg2-binary
"""

from __future__ import annotations
import os
import re
import sys
import argparse
from datetime import datetime
from typing import Optional, List, Tuple

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# -------------------------
# Load config / env
# -------------------------
# 1) Load local .env if exists (for standalone runs)
load_dotenv(dotenv_path="a.env")

# 2) Get DB credentials from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PW") or os.getenv("DB_PASSWORD")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")

# 3) Get DailyGammon login from environment variables
DG_LOGIN = os.getenv("DG_LOGIN")
DG_PW = os.getenv("DG_PW")

LOGIN_URL = "http://dailygammon.com/bg/login"
BASE_URL = "http://dailygammon.com/bg/game/{}/0/list"

# 4) sanity checks
if not (DB_HOST and DB_NAME and DB_USER and DB_PASSWORD):
    print("ERROR: DB connection data missing. Set DB_HOST, DB_NAME, DB_USER, DB_PW in env or a.env")
    sys.exit(1)

if not (DG_LOGIN and DG_PW):
    print("ERROR: DailyGammon login missing. Set DG_LOGIN and DG_PW in env or a.env")
    sys.exit(1)

# -------------------------
# helper: DB connection
# -------------------------
def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode=DB_SSLMODE,
    )


# -------------------------
# login session
# -------------------------
def login_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    payload = {"login": DG_LOGIN, "password": DG_PW, "save": "1"}
    r = s.post(LOGIN_URL, data=payload, timeout=30)
    r.raise_for_status()
    return s


# -------------------------
# scraping: get player matches for a season string
# -------------------------
def get_player_matches(session: requests.Session, player_id: int, season_str: Optional[str]) -> List[Tuple[str, str]]:
    """
    Returns list of tuples: (opponent_name_dg, match_id_str)
    season_str is used as substring filter on the <tr> text (case-insensitive).
    """
    url = f"http://www.dailygammon.com/bg/user/{player_id}"
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"⚠️ Error fetching {url}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    # take only rows that contain a game link
    rows = [tr for tr in soup.find_all("tr") if tr.find("a", href=re.compile(r"/bg/game/\d+/0/"))]
    res: List[Tuple[str, str]] = []

    for row in rows:
        text = row.get_text(" ", strip=True)
        if season_str and season_str.lower().strip() not in text.lower():
            continue

        opponent_link = row.find("a", href=re.compile(r"/bg/user/\d+"))
        match_link = row.find("a", href=re.compile(r"/bg/game/\d+/0/"))
        if not opponent_link or not match_link:
            continue

        opponent_name = opponent_link.get_text(" ", strip=True)
        m = re.search(r"/bg/game/(\d+)/0/", match_link["href"])
        if not m:
            continue
        match_id = m.group(1)
        res.append((opponent_name, match_id))
    return res


# -------------------------
# main logic: find & update match_ids
# -------------------------
def process_groups(season_to_use: str, league_to_use: Optional[str], do_commit: bool = True):
    """
    season_to_use: e.g. "34"
    league_to_use: e.g. "A1" or None -> process all groups in season
    do_commit: if False -> dry-run: do not write to DB
    """
    try:
        conn = connect_db()
    except Exception as e:
        print(f"ERROR: could not connect to DB: {e}")
        sys.exit(1)

    # Use RealDictCursor to access columns by name
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Determine groups to process
    if league_to_use:
        cur.execute(
            """
            SELECT group_id, saison_nummer, liga
            FROM groups
            WHERE saison_nummer = %s AND liga = %s
            """,
            (season_to_use, league_to_use),
        )
        groups = cur.fetchall()
    else:
        cur.execute(
            """
            SELECT group_id, saison_nummer, liga
            FROM groups
            WHERE saison_nummer = %s
            """,
            (season_to_use,),
        )
        groups = cur.fetchall()

    if not groups:
        print(f"ℹ️ No groups found for season='{season_to_use}' league='{league_to_use}'")
        conn.close()
        return

    # build season_substring similar to original streamlit:
    # if league provided, include it (e.g. "34th-season-A1"), otherwise "34th-season"
    if league_to_use:
        season_substring = f"{season_to_use}th-season-{league_to_use}"
    else:
        season_substring = f"{season_to_use}th-season"

    # login to DailyGammon
    try:
        session = login_session()
    except Exception as e:
        print(f"ERROR: could not login to DailyGammon: {e}")
        conn.close()
        sys.exit(1)

    print(f"▶ Logged into DailyGammon; processing {len(groups)} group(s) (season substring='{season_substring}')")

    # Preload existing match_ids to avoid duplicates
    cur.execute("SELECT match_id FROM matches WHERE match_id IS NOT NULL;")
    rows = cur.fetchall()
    existing_match_ids = set(row["match_id"] for row in rows if row.get("match_id") is not None)
    print(f"ℹ️ Loaded {len(existing_match_ids)} existing match_id(s) from DB.")

    total_found = 0
    total_updates = 0
    total_missing_before = 0

    for g in groups:
        group_id = g["group_id"]
        liga = g["liga"]
        saison_nummer = g["saison_nummer"]
        print(f"\n--- Processing group_id={group_id} (league={liga}, season={saison_nummer}) ---")

        # fetch missing matches for this group
        cur.execute(
            """
            SELECT m.id as match_pk, p1.dg_player_id as dg_player_id, p1.player_name AS player_name,
                   p2.player_name AS opponent_name
            FROM matches m
            JOIN players p1 ON m.player_id = p1.player_id
            JOIN players p2 ON m.opponent_id = p2.player_id
            WHERE m.group_id = %s AND m.match_id IS NULL;
            """,
            (group_id,),
        )
        missing = cur.fetchall()
        print(f"Found {len(missing)} missing match_id entries in DB for this group.")
        total_missing_before += len(missing)

        for row in missing:
            match_pk = row["match_pk"]
            dg_player_id = row["dg_player_id"]
            player_name_db = row["player_name"]
            opponent_name_db = row["opponent_name"]

            if dg_player_id is None:
                print(f" - Skipping {player_name_db} vs {opponent_name_db}: no dg_player_id")
                continue

            player_matches = get_player_matches(session, dg_player_id, season_substring)
            total_found += len(player_matches)
            saved = False

            for opponent_name_dg, match_id_str in player_matches:
                try:
                    mid = int(match_id_str)
                except Exception:
                    continue

                if mid in existing_match_ids:
                    print(f"   - match_id {mid} already exists elsewhere -> skipping")
                    continue

                # Determine switched_flag
                if opponent_name_dg.strip().lower() == opponent_name_db.strip().lower():
                    switched_flag = False
                elif opponent_name_dg.strip().lower() == player_name_db.strip().lower():
                    switched_flag = True
                else:
                    # Names do not match: skip this DG match
                    continue

                match_link = f"http://www.dailygammon.com/bg/game/{mid}/0/list#end"

                if do_commit:
                    try:
                        with conn:
                            with conn.cursor() as txcur:
                                txcur.execute(
                                    """
                                    UPDATE matches 
                                    SET match_id = %s, match_link = %s, switched_flag = %s
                                    WHERE id = %s;
                                    """,
                                    (mid, match_link, switched_flag, match_pk),
                                )
                        existing_match_ids.add(mid)
                        total_updates += 1
                        saved = True
                        print(f"✅ Saved match_id={mid} for {player_name_db} vs {opponent_name_db} (switched={switched_flag})")
                        break
                    except Exception as e:
                        print(f"⚠️ DB update failed for match_pk={match_pk}: {e}")
                else:
                    # dry-run: just print what would be done
                    print(f"[DRY-RUN] Would update match id {match_pk} -> match_id={mid} "
                          f"({player_name_db} vs {opponent_name_db}, switched={switched_flag})")
                    total_updates += 1
                    saved = True
                    break

            if not saved:
                print(f" - No valid match_id found on DG for {player_name_db} vs {opponent_name_db}")

    conn.close()
    print(f"\nFinished. total DG rows parsed: {total_found}, total DB updates (or planned): {total_updates}, missing before: {total_missing_before}")


# -------------------------
# helper to compute default season (max in DB)
# -------------------------
def get_max_season_from_db() -> Optional[str]:
    try:
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("SELECT MAX(saison_nummer) FROM groups;")
        row = cur.fetchone()
        conn.close()
        if row and row[0] is not None:
            return str(row[0])
    except Exception as e:
        print(f"ERROR reading max season from DB: {e}")
    return None


# -------------------------
# CLI
# -------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Find missing DailyGammon match_id and write to Neon DB.")
    p.add_argument("--season", "-s", help="Season number (e.g. 34). If omitted, uses MAX(saison_nummer) from DB.")
    p.add_argument("--league", "-l", help="Liga (exact string as in groups.liga). If omitted, process all groups in season.")
    p.add_argument("--dry-run", action="store_true", help="Do not write to DB; only show what would be updated.")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    season = args.season
    if not season:
        season = get_max_season_from_db()
        if not season:
            print("ERROR: Could not determine season from DB and none specified.")
            sys.exit(1)
        print(f"ℹ️ No --season given. Using highest season from DB: {season}")

    do_commit_flag = not args.dry_run
    if args.dry_run:
        print("** DRY RUN mode: no DB writes will be performed **")

    process_groups(season_to_use=season, league_to_use=args.league, do_commit=do_commit_flag)
