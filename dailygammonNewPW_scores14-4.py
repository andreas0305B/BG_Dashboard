"""
DailyGammon Score Synchronizer
------------------------------

This script synchronizes tournament match results between an Neon DB results table
and DailyGammon (DG). It automates the process of filling in missing match IDs,
fetching match results, and updating scores into the correct table cells.

This script processes match results for a specific league and writes them to Neon DB. 


Core Concepts:
--------------

1. Match ID Handling
   - If a match_id cell is empty, the script searches DG for invitations
     initiated by the "player" and inserts the found ID into Excel.
   - If a match_id cell is filled, it may be either:
        a) Automatically inserted earlier (normal case)
        b) Manually entered by a moderator (manual ID)
   - Manual IDs are detected by comparing player/opponent order between Excel
     and DG: if reversed, the entry is considered manual.

2. Manual Match IDs
   - Stored separately in `matches_by_hand`.
   - They carry a `switched=True` flag, meaning that for DG lookups the roles
     of player and opponent must be swapped to retrieve results.
   - When writing scores back to Excel, the swapped results are re-switched
     so the table remains consistent from the perspective of the Excel player.

3. Caching
   - Each match_id is requested from DG at most once.
   - A simple dict (`html_cache`) maps { match_id -> html } to reduce load.

4. Safety Rules
   - The script never overwrites an existing score of 11.

"""

import requests
from bs4 import BeautifulSoup
import os
import re
import streamlit as st
from dotenv import load_dotenv
import pandas as pd
import sys
import pytz
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor


# --- Login Data ---
load_dotenv(dotenv_path="a.env")  # lokale .env laden
login_url = "http://dailygammon.com/bg/login"

# Zuerst versuchen, aus Streamlit-Secrets zu laden, sonst .env / Umgebungsvariablen
try:
    DG_LOGIN = st.secrets["dailygammon"]["login"]
    DG_PW = st.secrets["dailygammon"]["password"]
except Exception:
    DG_LOGIN = os.getenv("DG_LOGIN", "")
    DG_PW = os.getenv("DG_PW", "")

# Prüfen, ob Login-Daten vorhanden sind
if not DG_LOGIN or not DG_PW:
    st.error("❌ Keine Login-Daten gefunden. Bitte in a.env oder .streamlit/secrets.toml eintragen.")
    st.stop()

payload = {
    "login": DG_LOGIN,
    "password": DG_PW,
    "save": "1"
}

BASE_URL = "http://dailygammon.com/bg/game/{}/0/list"

# Werte aus st.secrets (Cloud) oder os.getenv (lokal)

try:
    # Try to read from st.secrets
    postgres_conf = st.secrets["postgres"]
    DB_HOST = postgres_conf["host"]
    DB_NAME = postgres_conf["dbname"]
    DB_USER = postgres_conf["user"]
    DB_PASSWORD = postgres_conf["password"]
    DB_SSLMODE = postgres_conf.get("sslmode", "require")
except Exception:
    # Fallback if no secrets.toml found or not running under Streamlit
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PW")
    DB_SSLMODE = os.getenv("DB_SSLMODE", "require")

# Verbindung herstellen
conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    sslmode=DB_SSLMODE
)

cur = conn.cursor(cursor_factory=RealDictCursor)

# -----------------------
# Streamlit Config & Auswahl
# -----------------------
st.set_page_config(
    page_title="Backgammon Championship",
    layout="wide",
    initial_sidebar_state="auto"
)

# CSS für Streamlit Output
st.markdown(
    """
    <style>
    /* Alles nach oben drücken */
    section.main > div {
        padding-top: 0rem !important;
        margin-top: 0rem !important;
    }
    .main .block-container {
        max-width: 100% !important;
        padding-left: 1rem !important; 
        padding-right: 1rem !important;
        padding-top: 0rem !important;
        margin-top: 0rem !important;
    }

    /* Season Selectbox schmaler */
    div[data-baseweb="select"] {
        max-width: 8em !important;
    }

    /* Tabellen auf volle Breite */
    table {
        width: 100% !important;
    }

    /* Pandas Tabellen kompakt */
    table.dataframe th, table.dataframe td {
        padding: 4px 5px !important;
        line-height: 1.4em !important;
        font-size: 14px !important;
    }

    /* Match ID Matrix & Score Matrix */
    table.match-matrix, table.score-matrix {
        border-collapse: collapse;
        width: 100%;
        table-layout: fixed;
    }

    /* Standard-Zellen */
    table.match-matrix th, table.match-matrix td,
    table.score-matrix th, table.score-matrix td {
        border: 1px solid #ddd;
        padding: 4px;
        white-space: nowrap;
        width: 80px;
        text-align: center;  /* Standard: mittig */
    }

    /* Erste Spalte (Spielernamen) linksbündig + sticky */
    table.match-matrix th:first-child,
    table.match-matrix tbody th:first-child,
    table.score-matrix th:first-child,
    table.score-matrix tbody th:first-child {
        text-align: left;
        font-weight: bold;
        position: sticky;
        left: 0;
        z-index: 1;
    }

    /* Farben nach Systemmodus */
    @media (prefers-color-scheme: dark) {
        table.match-matrix th:first-child,
        table.match-matrix tbody th:first-child,
        table.score-matrix th:first-child,
        table.score-matrix tbody th:first-child {
            background-color: #000000;
            color: #ffffff;
        }
    }

    @media (prefers-color-scheme: light) {
        table.match-matrix th:first-child,
        table.match-matrix tbody th:first-child,
        table.score-matrix th:first-child,
        table.score-matrix tbody th:first-child {
            background-color: #f0f0f0;
            color: #000000;
        }
    }

    /* Header oben fixieren */
    table.match-matrix thead th, table.score-matrix thead th {
        position: sticky;
        top: 0;
        z-index: 2;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# -----------------------
# Header
# -----------------------
st.markdown(
    """
    <h1 style='text-align: center; color: #1F3A93; font-size: 3em;'>
        🎲 Backgammon Championship
    </h1>
    """,
    unsafe_allow_html=True
)

st.markdown("---")

# -------------------------------
# Fetch Seasons and Leagues/Groups
# -------------------------------
with conn.cursor() as cur:
    cur.execute("SELECT DISTINCT saison_nummer FROM groups ORDER BY saison_nummer DESC;")
    seasons = [str(row[0]) for row in cur.fetchall()]

# Default Season selection
col1, col2 = st.columns([0.6, 5])

with col1:
    season_input = st.selectbox("Season", seasons, index=0)

# Fetch leagues/groups for selected season
with conn.cursor() as cur:
    cur.execute("""
        SELECT liga
        FROM groups
        WHERE saison_nummer = %s
        ORDER BY liga;
    """, (season_input,))
    groups = [row[0] for row in cur.fetchall()]

with col2:
    # Show as radio buttons horizontally
    selection = st.radio("League + Group", groups, index=0, horizontal=True)

st.write(f"Selected Season: {season_input}, League/Group: {selection}")


# -----------------------
# Variablen für Script
# -----------------------
# Saison ist direkt aus Auswahl

saison_nummer = season_input

# Liga: entweder aus Wrapper (sys.argv) oder aus Streamlit
if len(sys.argv) > 1:
    liga = sys.argv[1]
else:
    liga = selection

# detect if script was called from wrapper with '--auto'
AUTO_MODE = "--auto" in sys.argv

# Einheitliche Dateinamen
file = f"{saison_nummer}th_Backgammon-championships_{liga}.xlsm"
season = f"{saison_nummer}th-season-{liga}"

# Initialisierung, damit sie immer existieren
df_players = None
df_matches = None
df_links = None

print(f"▶ Script started – collecting links and data for {season}")

# -----------------------------
# Read players from DB (filtered by season + league)
# -----------------------------
# -----------------------
# DB Query Helpers
# -----------------------

def run_query(query: str, params: tuple = None):
    import pandas as pd
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        print(f"⚠️ DB read failed: {e}")
        return pd.DataFrame()

def execute_query(query: str, params: tuple = None):
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
        conn.commit()
        return True
    except Exception as e:
        print(f"⚠️ DB write failed: {e}")
        conn.rollback()
        return False

# Hole group_id passend zur Auswahl
group_query = """
    SELECT group_id 
    FROM groups 
    WHERE saison_nummer = %s AND liga = %s;
"""
df_group = run_query(group_query, (saison_nummer, liga))

if df_group.empty:
    st.error(f"Keine Gruppe gefunden für Season {saison_nummer} und Liga {liga}.")
    st.stop()

GROUP_ID = int(df_group["group_id"].iloc[0])

# Jetzt Spieler der gewählten Gruppe laden
df_players = run_query("""
    SELECT p.player_id, p.player_name, p.player_link
    FROM players p
    JOIN player_groups pg ON pg.player_id = p.player_id
    WHERE pg.group_id = %s
    ORDER BY p.player_name;
""", (GROUP_ID,))

if df_players.empty:
    st.warning(f"Keine Spieler gefunden für Season {saison_nummer} / Liga {liga}.")
    st.stop()

players = df_players["player_name"].tolist()

print(f"▶ Streamlit started – analyzing group {GROUP_ID} ({saison_nummer}-{liga})")

# -----------------------------------------------------
# --- DataFrame für Link-Matrix (optional) ---
# -----------------------------------------------------

df_links_from_db = run_query("""
    SELECT
        m.match_id,
        p1.player_name AS player_name,
        p2.player_name AS opponent_name
    FROM matches m
    JOIN players p1 ON m.player_id = p1.player_id
    JOIN players p2 ON m.opponent_id = p2.player_id
    WHERE m.group_id = %s;
""", (GROUP_ID,))

df_links = pd.DataFrame(index=players, columns=players)
df_links_clickable = df_links.copy()  # später evtl. klickbare Links für Streamlit

# -----------------------------------------------------
# --- Data structures für Match-Verarbeitung ---
# -----------------------------------------------------
matches = {}            # match_id -> Match-Daten (gefüllt aus DB)
matches_by_hand = {}    # optional: manuell bearbeitete Matches
match_id_to_excel = {}  # nur relevant, falls später Excel erstellt wird
html_cache = {}         # optional: HTML-Caching für Spielerstatistiken
finished_by_id = {}     # match_id -> finished boolean

# -----------------------------------------------------
# --- Login session ---
# -----------------------------------------------------
# Purpose:
#   Opens a persistent HTTP session with DailyGammon,
#   logs in with your credentials, and returns the session
#   so all following requests are authenticated.
# -----------------------------------------------------
def login_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    resp = s.post(login_url, data=payload, timeout=30)
    resp.raise_for_status()
    return s
session = login_session()

# -----------------------------------------------------
# --- Collect matches per player ---
# -----------------------------------------------------
# -----------------------------------------------------
# Purpose:
#   Collects all matches for a specific player in the
#   given season. It scrapes the DailyGammon user page
#   and extracts:
#     - Opponent name
#     - Opponent ID
#     - Match ID
# -----------------------------------------------------

def get_player_matches(session: requests.Session, player_id, season):
    """
    Return list of tuples: (opponent_name_dg, match_id)
    - Only parses <tr> rows that contain the target season and a /bg/game/.../0/ link.
    """
    url = f"http://www.dailygammon.com/bg/user/{player_id}"
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"⚠️ Error fetching {url}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    player_matches = []

    # Fetch all <tr> rows that contain a match link
    rows = [tr for tr in soup.find_all("tr") if tr.find("a", href=re.compile(r"/bg/game/\d+/0/"))]
    print(f"🌐 GET {url} -> Found {len(rows)} relevant <tr> rows; season='{season}'")

    for i, row in enumerate(rows, start=1):
        text = row.get_text(" ", strip=True)

        # Only keep rows with the season string
        if season and season.lower().strip() not in text.lower():
            continue

        opponent_link = row.find("a", href=re.compile(r"/bg/user/\d+"))
        match_link = row.find("a", href=re.compile(r"/bg/game/\d+/0/"))
        if not opponent_link or not match_link:
            continue

        opponent_name_dg = opponent_link.get_text(" ", strip=True)
        match_id = re.search(r"/bg/game/(\d+)/0/", match_link["href"]).group(1)

        print(f"   + found DG match: opponent_name_dg='{opponent_name_dg}', match_id={match_id}")
        player_matches.append((opponent_name_dg, match_id))

    print(f"✅ Parsed {len(player_matches)} match(es) from DG for player_id={player_id}")
    return player_matches

# -----------------------------------------------------
# --- Helper functions: fetch HTML & extract scores ---
# -----------------------------------------------------
# Purpose:
#   Downloads the HTML page for a specific match ID.
#   Returns the HTML content or None if the request failed.
# -----------------------------------------------------

def fetch_list_html(session: requests.Session, match_id: int) -> str | None:
    url = BASE_URL.format(match_id)
    try:
        resp = session.get(url, timeout=30)
        if not resp.ok or "Please Login" in resp.text:
            return None
        return resp.text
    except requests.RequestException:
        return None

# -----------------------------------------------------
# Function: extract_latest_score
# Purpose:
#   Parses the match HTML page and extracts the latest
#   visible score row for the two players.
#   Returns player names + current scores.
#
# PARSE LATEST SCORE FROM MATCH PAGE:
# - Scans table rows from bottom to top (reversed) to find the most recent score line.
# - Assumes the pattern "<Name> : <Score>" is present on both left and right columns.
# -----------------------------------------------------

def extract_latest_score(html: str, players_list: list[str]):
    soup = BeautifulSoup(html, "html.parser")
    for row in reversed(soup.find_all("tr")):
        text = row.get_text(" ", strip=True)
        if not any(p in text for p in players_list):
            continue
        cells = row.find_all("td")
        if len(cells) >= 3:
            left_text = cells[1].get_text(" ", strip=True)
            right_text = cells[2].get_text(" ", strip=True)
            left_match = re.match(r"(.+?)\s*:\s*(\d+)", left_text)
            right_match = re.match(r"(.+?)\s*:\s*(\d+)", right_text)
            if left_match and right_match:
                left_name, left_score = left_match.groups()
                right_name, right_score = right_match.groups()
                return left_name.strip(), right_name.strip(), int(left_score), int(right_score)
    return None

# -----------------------------------------------------
# Function: map_scores
# Purpose:
#   Aligns scores from DailyGammon with the correct order
#   for storing in the DB.
#   Handles switched cases (player order reversed for manually added matches).
#
# NAME/SCORE ALIGNMENT:
# - 'switched_flag=True' means the match was manually entered with reversed order,
#   so we swap scores here.
# - If names match exactly (case-insensitive), we map directly; otherwise
#   we use a small heuristic (substring check) as a fallback.
#   If unsure, return None (skip update).
# -----------------------------------------------------

def map_scores(player, opponent, left_name, right_name, left_score, right_score, switched_flag):
    ln = left_name.strip().lower()
    rn = right_name.strip().lower()
    pn = player.strip().lower()
    on = opponent.strip().lower()

    if switched_flag:
        return right_score, left_score

    if ln == pn and rn == on:
        return left_score, right_score
    if ln == on and rn == pn:
        return right_score, left_score

    # Fallback heuristic if names differ slightly
    if pn in ln or pn in rn or on in ln or on in rn:
        if pn in ln:
            return left_score, right_score
        if pn in rn:
            return right_score, left_score
    return None

    # =============================
    # Custom CSS für CI
    # =============================
    st.markdown(
        """
        <style>
        /* Tabs allgemein */
        .stTabs [role="tablist"] {
            background-color: #F8F9F9;
            border-radius: 12px;
            padding: 6px;
            gap: 10px;
        }

        /* Einzelne Tabs */
        .stTabs [role="tab"] {
            background-color: #ffffff;
            color: #333333;
            border-radius: 8px;
            padding: 6px 12px;
            transition: none;
        }

        /* Kein Hover-Effekt */
        .stTabs [role="tab"]:hover {
            background-color: #ffffff !important;
            color: #333333 !important;
        }

        /* Aktiver Tab */
        .stTabs [aria-selected="true"] {
            background-color: #EAEDED !important;
            color: #000000 !important;
            font-weight: 600;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

# -----------------------
# DB-only Streamlit Output für alle Tabs
# -----------------------
tab1, tab2, tab3 = st.tabs(["League Table", "Score Matrix", "Match ID Matrix"])

# Platzhalter nur einmal im session_state anlegen
if "dg_placeholders" not in st.session_state:
    st.session_state.dg_placeholders = {
        "tab1": tab1.empty(),
        "tab2": tab2.empty(),
        "tab3": tab3.empty(),
    }

placeholder_tab1 = st.session_state.dg_placeholders["tab1"]
placeholder_tab2 = st.session_state.dg_placeholders["tab2"]
placeholder_tab3 = st.session_state.dg_placeholders["tab3"]

# --- Tab 1: League Table ---
# --- Load players and matches directly from Neon DB ---
with conn.cursor() as cur:
    # Get all players
    cur.execute("""
        SELECT p.player_name, p.player_link
        FROM players p
        JOIN player_groups pg ON pg.player_id = p.player_id
        WHERE pg.group_id = %s
        ORDER BY p.player_name;
    """, (GROUP_ID,))

    player_rows = cur.fetchall()
    players = [r[0] for r in player_rows]
    player_links = {r[0]: r[1] for r in player_rows}

    # Get all finished matches with joined player names (nur für ausgewählte Gruppe)
    cur.execute("""
        SELECT 
            m.match_id,
            p1.player_name AS player_name,
            p2.player_name AS opponent_name,
            m.left_score,
            m.right_score,
            m.finished
        FROM matches m
        JOIN players p1 ON m.player_id = p1.player_id
        JOIN players p2 ON m.opponent_id = p2.player_id
        WHERE m.group_id = %s;
    """, (GROUP_ID,))
    match_rows = cur.fetchall()

# Build intermediate score map (robust gegen NULL-Werte)
intermediate_scores = {}

for row in match_rows:
    match_id, player, opponent, left_score, right_score, finished = row

    # Wenn Scores NULL sind → 0 setzen
    s_player = int(left_score) if left_score is not None else 0
    s_opponent = int(right_score) if right_score is not None else 0

    intermediate_scores[(player, opponent)] = (s_player, s_opponent)

# Build League Stats
num_players = len(players)
total_matches_per_player = (num_players - 1) * 2
stats = []

for player in players:
    finished_matches = 0
    all_plus = all_minus = all_total = 0
    finished_plus = finished_minus = finished_total = 0
    for opponent in players:
        if player == opponent:
            continue
        key_lr = (player, opponent)
        if key_lr in intermediate_scores:
            s_player, s_opponent = intermediate_scores[key_lr]
            all_plus += s_player
            all_minus += s_opponent
            all_total += s_player - s_opponent
            if s_player == 11 or s_opponent == 11:
                finished_matches += 1
                finished_plus += s_player
                finished_minus += s_opponent
                finished_total += s_player - s_opponent
        key_rl = (opponent, player)
        if key_rl in intermediate_scores:
            s_opp, s_player = intermediate_scores[key_rl]
            all_plus += s_player
            all_minus += s_opp
            all_total += s_player - s_opp
            if s_player == 11 or s_opp == 11:
                finished_matches += 1
                finished_plus += s_player
                finished_minus += s_opp
                finished_total += s_player - s_opp
    won = sum(
        1
        for (p, o), (sp, so) in intermediate_scores.items()
        if ((p == player and sp > so) or (o == player and so > sp)) and (sp == 11 or so == 11)
    )
    lost = sum(
        1
        for (p, o), (sp, so) in intermediate_scores.items()
        if ((p == player and sp < so) or (o == player and so < sp)) and (sp == 11 or so == 11)
    )
    pct_won = round((won / (won + lost) * 100)) if (won + lost) > 0 else "---"
    stats.append(
        [
            player,
            f"{finished_matches}/{total_matches_per_player}",
            won,
            lost,
            pct_won,
            all_plus,
            all_minus,
            all_total,
            finished_plus,
            finished_minus,
            finished_total,
        ]
    )

df_stats = pd.DataFrame(
    stats,
    columns=[
        "Player",
        "Finished",
        "Won",
        "Lost",
        "% Won",
        "All +",
        "All -",
        "All Total",
        "Finished +",
        "Finished -",
        "Finished Total",
    ],
)

# MultiIndex-Spalten
multi_cols = pd.MultiIndex.from_tuples(
    [
        ("", "Player"),
        ("", "Finished"),
        ("", "Won"),
        ("", "Lost"),
        ("", "% Won"),
        ("All matches", "+"),
        ("All matches", "-"),
        ("All matches", "Total"),
        ("Finished matches", "+"),
        ("Finished matches", "-"),
        ("Finished matches", "Total"),
    ]
)
df_stats.columns = multi_cols

# Spielernamen zu Links machen
df_stats[("", "Player")] = df_stats[("", "Player")].apply(
    lambda p: f'<a href="{player_links.get(p, "#")}" target="_blank">{p}</a>'
)

# Numerische Spalten für sort
df_stats[("", "Won")] = pd.to_numeric(df_stats[("", "Won")], errors="coerce").fillna(0)
df_stats[("Finished matches", "Total")] = pd.to_numeric(
    df_stats[("Finished matches", "Total")], errors="coerce"
).fillna(0)
df_stats[("Finished matches", "+")] = pd.to_numeric(
    df_stats[("Finished matches", "+")], errors="coerce"
).fillna(0)

df_stats = df_stats.sort_values(
    by=[("", "Won"), ("Finished matches", "Total"), ("Finished matches", "+")],
    ascending=[False, False, False],
).reset_index(drop=True)

with conn.cursor() as cur:
    cur.execute("SELECT last_updated FROM groups WHERE group_id = %s;", (GROUP_ID,))
    row = cur.fetchone()
    last_updated_dt = row[0] if row and row[0] else datetime.now(pytz.timezone("Europe/Berlin"))

# Tabelle in Platzhalter schreiben
with tab1:
    df_stats_html = df_stats.to_html(escape=False, index=False)
    formatted_time = last_updated_dt.astimezone(pytz.timezone("Europe/Berlin")).strftime("%b %d, %Y %H:%M %Z")
    html = df_stats_html + f"<p style='font-size:12px; color:gray;'>Last updated: {formatted_time}</p>"
    placeholder_tab1.markdown(html, unsafe_allow_html=True)

# --- Tab 2: Score Matrix ---
players = sorted(players)
matrix_scores = pd.DataFrame("", index=players, columns=players)

# Fetch all finished matches for the selected group (filtered by GROUP_ID)
with conn.cursor() as cur:
    cur.execute("""
        SELECT 
            m.match_id,
            p1.player_name AS player_name,
            p2.player_name AS opponent_name,
            m.left_score,
            m.right_score
        FROM matches m
        JOIN players p1 ON m.player_id = p1.player_id
        JOIN players p2 ON m.opponent_id = p2.player_id
        WHERE m.group_id = %s;
    """, (GROUP_ID,))
    match_rows = cur.fetchall()

# Fill matrix with clickable score links
for match_id, player, opponent, left_score, right_score in match_rows:
    if left_score is not None and right_score is not None:
        matrix_scores.at[player, opponent] = (
            f'<a href="http://dailygammon.com/bg/game/{int(match_id)}/0/list#end" target="_blank">'
            f"{int(left_score)} : {int(right_score)}</a>"
        )

# Reindex to enforce alphabetical order (rows & columns)
matrix_scores = matrix_scores.reindex(index=players, columns=players)

# Minimaler Eingriff für linke Spalte als <th> und eigene CSS-Klasse
html_table = matrix_scores.to_html(escape=False)
html_table = (
    html_table.replace("<tr><td>", "<tr><th>")
    .replace("</td></tr>", "</th></tr>")
    .replace('<table border="1" class="dataframe">', '<table class="score-matrix">')
)

placeholder_tab2.markdown(html_table, unsafe_allow_html=True)

# --- Tab 3: Match ID Matrix ---
df_links_clickable = pd.DataFrame("", index=players, columns=players)

with conn.cursor() as cur:
    cur.execute("""
        SELECT 
            m.match_id,
            p1.player_name AS player_name,
            p2.player_name AS opponent_name
        FROM matches m
        JOIN players p1 ON m.player_id = p1.player_id
        JOIN players p2 ON m.opponent_id = p2.player_id
        WHERE m.group_id = %s;
    """, (GROUP_ID,))
    match_rows = cur.fetchall()

# Fill matrix directly
for match_id, player, opponent in match_rows:
    if match_id is not None:
        try:
            match_id_int = int(match_id)
            df_links_clickable.at[player, opponent] = (
                f'<a href="http://dailygammon.com/bg/game/{match_id_int}/0/list#end" '
                f'target="_blank">{match_id_int}</a>'
            )
        except (TypeError, ValueError):
            df_links_clickable.at[player, opponent] = ""
    else:
        df_links_clickable.at[player, opponent] = ""

# Render
html_table = df_links_clickable.to_html(escape=False)
html_table = html_table.replace(
    '<table border="1" class="dataframe">', 
    '<table class="match-matrix">'
)
placeholder_tab3.markdown(html_table, unsafe_allow_html=True)

# Prüfen, ob noch leere Zellen existieren
if (df_links_clickable == "").any().any():
    needs_refresh = True
else:
    needs_refresh = False


# -----------------------------------------------------
# Step 2 old: Fill missing match IDs from DailyGammon (DB version)
# -----------------------------------------------------

#with conn.cursor() as cur:
#    cur.execute("""
#        SELECT 
#            m.id AS match_pk,
#            p1.dg_player_id AS dg_player_id,
#            p1.player_name AS player_name,
#            p2.player_name AS opponent_name_db
#        FROM matches m
#        JOIN players p1 ON m.player_id = p1.player_id
#        JOIN players p2 ON m.opponent_id = p2.player_id
#        WHERE m.group_id = %s AND m.match_id IS NULL;
#    """, (GROUP_ID,))
#    missing_matches = cur.fetchall()
#
#if not missing_matches:
#    print("ℹ️ All matches already have match_id, skipping fetch.")
#else:
#    # Fetch all existing match_ids once to avoid duplicates
#    with conn.cursor() as cur:
#        cur.execute("SELECT match_id FROM matches WHERE match_id IS NOT NULL;")
#        existing_match_ids = set(r[0] for r in cur.fetchall())
#
#    for match_pk, dg_player_id, player_name, opponent_name_db in missing_matches:
#        player_matches = get_player_matches(session, dg_player_id, season=season)
#
#        for opponent_name_dg, match_id in player_matches:
#            # only fill null match_ids if the DG match_id is not yet used
#            if opponent_name_dg.strip().lower() != opponent_name_db.strip().lower():
#                continue
#
#            try:
#                mid_int = int(match_id)
#            except (TypeError, ValueError):
#                continue
#
#            if mid_int in existing_match_ids:
#                continue
#
#            # safe to update
#            key = (player_name, opponent_name_db)
#            matches[key] = mid_int
#            matches_by_hand[key] = (mid_int, False)
#
#            with conn.cursor() as cur:
#                cur.execute("""
#                    UPDATE matches
#                    SET match_id = %s
#
#                     WHERE id = %s;
#                """, (mid_int, match_pk))
#                conn.commit()
#
#            existing_match_ids.add(mid_int)
#            print(f"🟢 Added missing match {player_name} vs {opponent_name_db} — match_id={mid_int}")
#            break  # found and saved — stop searching this pair
#
#    print("✅ Match IDs updated for missing entries.")


# -----------------------------------------------------
# Step 2: Fill missing match IDs if needed
# -----------------------------------------------------
if not needs_refresh:
    print("ℹ️ All matches already have match_id, skipping fetch.")
else:
    print("🔄 Missing match_ids detected — fetching updates from DailyGammon...")

    # Fetch all existing match_ids once to avoid duplicates
    with conn.cursor() as cur:
        cur.execute("SELECT match_id FROM matches WHERE match_id IS NOT NULL;")
        existing_match_ids = set(r[0] for r in cur.fetchall())

    # Fetch all matches that have match_id = NULL
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                m.id AS match_pk,
                p1.dg_player_id AS dg_player_id,
                p1.player_name AS player_name,
                p2.player_name AS opponent_name_db
            FROM matches m
            JOIN players p1 ON m.player_id = p1.player_id
            JOIN players p2 ON m.opponent_id = p2.player_id
            WHERE m.group_id = %s AND m.match_id IS NULL;
        """, (GROUP_ID,))
        missing_matches = cur.fetchall()

    for match_pk, dg_player_id, player_name, opponent_name_db in missing_matches:
        player_matches = get_player_matches(session, dg_player_id, season=season)

        for opponent_name_dg, match_id in player_matches:
            if opponent_name_dg.strip().lower() != opponent_name_db.strip().lower():
                continue

            try:
                mid_int = int(match_id)
            except (TypeError, ValueError):
                continue

            if mid_int in existing_match_ids:
                continue

            # safe to update
            key = (player_name, opponent_name_db)
            matches[key] = mid_int
            matches_by_hand[key] = (mid_int, False)

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE matches
                    SET match_id = %s
                    WHERE id = %s;
                """, (mid_int, match_pk))
                conn.commit()

            existing_match_ids.add(mid_int)
            print(f"🟢 Added missing match {player_name} vs {opponent_name_db} — match_id={mid_int}")
            break  # found and saved — stop searching this pair

    print("✅ Match IDs updated for missing entries.")

# -----------------------------------------------------
# Build mapping directly from Neon DB
# -----------------------------------------------------
# Dictionary: match_id -> (player_name, opponent_name, switched_flag)
match_id_to_db = {}

# Fetch all matches from DB for the selected group
with conn.cursor() as cur:
    cur.execute("""
        SELECT 
            m.match_id,
            p1.player_name AS player_name,
            p2.player_name AS opponent_name,
            m.switched_flag
        FROM matches m
        JOIN players p1 ON m.player_id = p1.player_id
        JOIN players p2 ON m.opponent_id = p2.player_id
        WHERE m.group_id = %s;
    """, (GROUP_ID,))
    match_rows = cur.fetchall()

# Fill mapping
for match_id, player_name, opponent_name, switched_flag in match_rows:
    if match_id is not None:
        match_id_to_db[int(match_id)] = (player_name, opponent_name, bool(switched_flag))


# -----------------------------------------------------
# Step 1: Check existing matches with HTML fetch (DB version)
# -----------------------------------------------------

# Build dictionaries for tracking
matches = {}          # (player, opponent) -> match_id
matches_by_hand = {}  # manual flipped matches
finished_by_id = finished_by_id if "finished_by_id" in locals() else {}
html_cache = html_cache if "html_cache" in locals() else {}

# Collect all match_ids not yet cached or finished
to_fetch_ids = [
    mid for mid in match_id_to_db.keys()
    if mid not in html_cache and mid not in finished_by_id
]

# Fetch HTML for all these matches
for match_id in to_fetch_ids:
    html_cache[match_id] = fetch_list_html(session, match_id)

# Evaluate matches
for match_id, (player_name, opponent_name, switched_flag) in match_id_to_db.items():

    # Skip finished matches
    if match_id in finished_by_id:
        matches[(player_name, opponent_name)] = match_id
        continue

    html = html_cache.get(match_id)
    if not html:
        matches[(player_name, opponent_name)] = match_id
        continue

    score_info = extract_latest_score(html, [player_name, opponent_name])
    if not score_info:
        matches[(player_name, opponent_name)] = match_id
        continue

    left_name, right_name, status, _ = score_info
    ln, rn = left_name.lower(), right_name.lower()
    pn, on = player_name.lower(), opponent_name.lower()

    if status == "finished":
        finished_by_id[match_id] = True

    # Normal order
    if ln == pn and rn == on:
        matches[(player_name, opponent_name)] = match_id
        match_id_to_db[match_id] = (player_name, opponent_name, False)
    # Swapped order
    elif ln == on and rn == pn:
        matches_by_hand[(player_name, opponent_name)] = (match_id, True)
        match_id_to_db[match_id] = (player_name, opponent_name, True)
        print(f"🟡 Manual match detected: {player_name} vs {opponent_name} (match_id={match_id})")
    # Unclear order
    else:
        matches[(player_name, opponent_name)] = match_id
        match_id_to_db[match_id] = (player_name, opponent_name, False)
        print(f"⚠️ Unclear order for match_id={match_id}: DG shows '{left_name}' vs '{right_name}'")


# -----------------------------------------------------
# Step 3 (DB): Collect finished matches directly from DB match_ids
# -----------------------------------------------------

# Ensure helper dicts exist
html_cache = html_cache if "html_cache" in globals() else {}
finished_by_id = finished_by_id if "finished_by_id" in globals() else {}
all_match_ids = {}

# -----------------------------------------------------
# Build mapping: match_id -> {player, opponent, winner}
# -----------------------------------------------------

# Load matches from Neon DB
with conn.cursor() as cur:
    cur.execute("""
        SELECT 
            m.match_id,
            p1.player_name AS player_name,
            p2.player_name AS opponent_name
        FROM matches m
        JOIN players p1 ON m.player_id = p1.player_id
        JOIN players p2 ON m.opponent_id = p2.player_id
        WHERE m.group_id = %s;
    """, (GROUP_ID,))
    rows = cur.fetchall()

# Map match_id -> player/opponent (winner wird später berechnet)
all_match_ids = {}
for match_id, player_name, opponent_name in rows:
    if match_id is None:
        continue
    all_match_ids[int(match_id)] = {
        "player": player_name,
        "opponent": opponent_name,
        "winner": None
    }

# -----------------------------------------------------
# Phase 1: Fetch export pages & detect winners from bottom
# -----------------------------------------------------
print("🔎 Phase 1: Fetch export pages & detect winners ...")

for match_id, row_data in all_match_ids.items():
    # Skip if already processed
    if row_data.get("winner"):
        continue

    export_url = f"http://www.dailygammon.com/bg/export/{match_id}"

    try:
        resp_export = session.get(export_url, timeout=30)
        resp_export.raise_for_status()
        text_lines = resp_export.text.splitlines()  # Zeilenweise aufteilen
    except requests.RequestException:
        continue

    # --- Winner detection heuristic (Excel-style, bottom-up) ---
    winner = None
    mid_threshold = 24  # Position der "Wins" entscheidet links/rechts

    for line in reversed(text_lines):  # von unten nach oben iterieren
        if "and the match" in line and "Wins" in line:
            pos = line.find("Wins")
            winner = row_data["player"] if pos < mid_threshold else row_data["opponent"]
            break  # nur die letzte relevante Zeile nutzen

    # --- Save winner info ---
    if winner:
        finished_by_id[match_id] = winner
        row_data["winner"] = winner
    else:
        print(f"⚠️ No winner found in export for match_id={match_id}")

print("🏁 Phase 1 completed (winners detected).")

# -----------------------------------------------------
# Phase 1 (DB): Write intermediate scores
# Purpose:
#   For each match, download the latest score and update
#   the "matches" table in Neon DB.
#   IMPORTANT: If a score of 11 is already present,
#   the match is considered finished and will not be overwritten.
# -----------------------------------------------------
# -----------------------------------------------------
# Collect all players from DB (nur für aktuelle Gruppe)
# -----------------------------------------------------
with conn.cursor() as cur:
    cur.execute("""
        SELECT DISTINCT p.player_name
        FROM players p
        JOIN matches m ON p.player_id IN (m.player_id, m.opponent_id)
        WHERE m.group_id = %s;
    """, (GROUP_ID,))
    players_in_matches = sorted([row[0] for row in cur.fetchall()])


# -----------------------------------------------------
# Helper: Update match scores directly in DB
# -----------------------------------------------------
def update_score_in_db(player_name, opponent_name, player_score, opponent_score, switched_flag):
    """
    Updates the score in the Neon DB for a specific match.
    Skips if match already finished (score 11).
    """
    try:
        with conn.cursor() as cur:
            # Check if match exists for this group
            cur.execute("""
                SELECT m.id, m.left_score, m.right_score
                FROM matches m
                JOIN players p1 ON m.player_id = p1.player_id
                JOIN players p2 ON m.opponent_id = p2.player_id
                WHERE p1.player_name = %s AND p2.player_name = %s
                AND m.group_id = %s;
            """, (player_name, opponent_name, GROUP_ID))
            row = cur.fetchone()
            if not row:
                return False

            match_pk, left_score, right_score = row

            # Do not overwrite finished matches
            if left_score == 11 or right_score == 11:
                return False

            # Update match score
            cur.execute("""
                UPDATE matches
                SET left_score = %s, right_score = %s
                WHERE id = %s;
            """, (player_score, opponent_score, match_pk))
            conn.commit()
            return True
    except Exception as e:
        return False

# -----------------------------------------------------
# Iterate over matches and refresh scores from HTML
# -----------------------------------------------------
for match_id, (db_player, db_opponent, switched_flag) in list(match_id_to_db.items()):
    # 🔍 1. Check if match already finished (nur für aktuelle Gruppe)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT left_score, right_score
            FROM matches m
            JOIN players p1 ON m.player_id = p1.player_id
            JOIN players p2 ON m.opponent_id = p2.player_id
            WHERE m.match_id = %s
            AND p1.player_name = %s
            AND p2.player_name = %s
            AND m.group_id = %s;
        """, (match_id, db_player, db_opponent, GROUP_ID))
        row = cur.fetchone()

    if row:
        left_score, right_score = row
        if left_score == 11 or right_score == 11:
            # ✅ Already finished → skip fetch
            matches[(db_player, db_opponent)] = match_id
            continue

    # 🔍 2. Fetch HTML if still open
    html = html_cache.get(match_id)
    if not html:
        html = fetch_list_html(session, match_id)
        html_cache[match_id] = html
    if not html:
        continue

    # Extract latest score
    result = extract_latest_score(html, players_in_matches)
    if not result:
        continue

    left_name, right_name, left_score, right_score = result

    
    # Map scores according to player names using existing map_scores()
    mapped = map_scores(db_player, db_opponent, left_name, right_name, left_score, right_score, switched_flag)
    if mapped is None:
        continue

    player_score, opponent_score = mapped

    # Update scores directly in Neon DB
    update_score_in_db(db_player, db_opponent, player_score, opponent_score, switched_flag)

# -----------------------------------------------------
# Phase 2 (DB): Final results – Set winners to 11 points
# -----------------------------------------------------

def update_match_score_in_db(match_id: int, winner_name: str, conn, group_id=None):
    """
    Set winner score to 11 in the DB for a given match_id.
    - winner_name: canonical name (already from DB, via Phase 1)
    - group_id: optional safeguard to limit update to current group
    """
    with conn.cursor() as cur:
        # Fetch switched_flag and canonical player/opponent names from DB
        if group_id is not None:
            cur.execute("""
                SELECT m.switched_flag, p1.player_name, p2.player_name
                FROM matches m
                JOIN players p1 ON m.player_id = p1.player_id
                JOIN players p2 ON m.opponent_id = p2.player_id
                WHERE m.match_id = %s AND m.group_id = %s;
            """, (match_id, group_id))
        else:
            cur.execute("""
                SELECT m.switched_flag, p1.player_name, p2.player_name
                FROM matches m
                JOIN players p1 ON m.player_id = p1.player_id
                JOIN players p2 ON m.opponent_id = p2.player_id
                WHERE m.match_id = %s;
            """, (match_id,))
        row = cur.fetchone()

        if not row:
            return

        switched_flag, db_player_name, db_opponent_name = row
        switched_flag = bool(switched_flag)

        # Determine which column (left/right) corresponds to the winner
        if winner_name == db_player_name:
            # left player in DB schema
            col = "left_score" if not switched_flag else "right_score"
        elif winner_name == db_opponent_name:
            # right player in DB schema
            col = "right_score" if not switched_flag else "left_score"
        else:
            return

        # Update the DB: set winner column to 11 and finished flag to TRUE
        cur.execute(f"""
            UPDATE matches
            SET {col} = 11, finished = TRUE
            WHERE match_id = %s;
        """, (match_id,))
        conn.commit()

print("🔎 Phase 2 (DB): Final results (set winner = 11) ...")

for match_id, winner_name in finished_by_id.items():
    # Gewinner ist immer ein String (bereits aus Phase 1)
    # Hole switched_flag, player und opponent aus der DB
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.switched_flag, p1.player_name, p2.player_name
            FROM matches m
            JOIN players p1 ON m.player_id = p1.player_id
            JOIN players p2 ON m.opponent_id = p2.player_id
            WHERE m.match_id = %s;
        """, (match_id,))
        row = cur.fetchone()

    if not row:
        continue

    switched_flag, db_player, db_opponent = row

    # Entscheide, ob links oder rechts 11 bekommt
    if winner_name == db_player:
        col = "left_score" if not switched_flag else "right_score"
    elif winner_name == db_opponent:
        col = "right_score" if not switched_flag else "left_score"
    else:
        continue

    # Direkter DB-Update
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE matches
            SET {col} = 11, finished = TRUE
            WHERE match_id = %s;
        """, (match_id,))
        conn.commit()

print("🏁 Phase 2 completed (DB updated).")

# -----------------------
# DB Query Helpers (improved)
# -----------------------

def run_query(query: str, params: tuple = None):
    """
    Executes a SELECT query on Neon DB and returns a pandas DataFrame.
    Automatically rolls back on failure so that later queries can continue.
    """
    import pandas as pd
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        print(f"⚠️ DB read failed: {e}")
        try:
            conn.rollback()   # 🧹 ensure transaction is reset
        except Exception as rollback_err:
            print(f"⚠️ Rollback failed: {rollback_err}")
        return pd.DataFrame()  # return empty df to keep pipeline alive


def execute_query(query: str, params: tuple = None):
    """
    Executes INSERT/UPDATE/DELETE queries on Neon DB and commits changes.
    Automatically rolls back on failure.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
        conn.commit()
        return True
    except Exception as e:
        print(f"⚠️ DB write failed: {e}")
        try:
            conn.rollback()
        except Exception as rollback_err:
            print(f"⚠️ Rollback failed: {rollback_err}")
        return False

# -----------------------
# Load Data from Neon DB for selected GROUP_ID
# -----------------------

df_players = run_query("""
    SELECT p.player_id, p.player_name, p.player_link
    FROM players p
    JOIN player_groups pg ON pg.player_id = p.player_id
    WHERE pg.group_id = %s
    ORDER BY p.player_name;
""", (GROUP_ID,))

df_matches = run_query("""
    SELECT 
        m.match_id,
        m.left_score,
        m.right_score,
        m.finished,
        m.switched_flag,
        p1.player_name AS player_name,
        p2.player_name AS opponent_name
    FROM matches m
    JOIN players p1 ON m.player_id = p1.player_id
    JOIN players p2 ON m.opponent_id = p2.player_id
    WHERE m.group_id = %s;
""", (GROUP_ID,))

df_links = run_query("""
    SELECT
        m.match_id,
        p1.player_name AS player_name,
        p2.player_name AS opponent_name
    FROM matches m
    JOIN players p1 ON m.player_id = p1.player_id
    JOIN players p2 ON m.opponent_id = p2.player_id
    WHERE m.group_id = %s;
""", (GROUP_ID,))


# Ensure consistent index structure (for Streamlit tabs)
if "player_name" in df_matches.columns:
    df_matches = df_matches.set_index("player_name")
if "player_name" in df_links.columns:
    df_links = df_links.set_index("player_name")

df_matches.index.name = None
df_links.index.name = None

# -----------------------
# CSS for Streamlit Tabs (unchanged)
# -----------------------
st.markdown("""
<style>
div[role="tab"] { font-size: 14px !important; padding: 4px 8px !important; }
div[role="tablist"] { margin-bottom: 10px !important; }
</style>
""", unsafe_allow_html=True)


# -----------------------
# Tabs aktualisieren (Neon DB Version) – angepasst an deine DB-Struktur
# -----------------------

with tab2:
    # 🔹 Matches mit Spielernamen (JOIN auf players) für ausgewählte GROUP_ID
    df_matches_from_db = run_query("""
        SELECT
            m.match_id,
            m.left_score,
            m.right_score,
            m.finished,
            m.switched_flag,
            p1.player_name AS player_name,
            p2.player_name AS opponent_name
        FROM matches m
        JOIN players p1 ON m.player_id = p1.player_id
        JOIN players p2 ON m.opponent_id = p2.player_id
        WHERE m.group_id = %s;
    """, (GROUP_ID,))

    # 🔹 Links für ausgewählte GROUP_ID
    df_links_from_db = run_query("""
        SELECT
            m.match_id,
            p1.player_name AS player_name,
            p2.player_name AS opponent_name
        FROM matches m
        JOIN players p1 ON m.player_id = p1.player_id
        JOIN players p2 ON m.opponent_id = p2.player_id
        WHERE m.group_id = %s;
    """, (GROUP_ID,))

    # 🔹 Spielerliste für ausgewählte GROUP_ID
    df_players = run_query("""
        SELECT p.player_name
        FROM players p
        JOIN player_groups pg ON pg.player_id = p.player_id
        WHERE pg.group_id = %s
        ORDER BY p.player_name;
    """, (GROUP_ID,))

    # --- Robust extrahieren ---
    if "player_name" in df_players.columns:
        players = df_players["player_name"].astype(str).tolist()
    elif df_players.shape[1] >= 1:
        players = df_players.iloc[:, 0].astype(str).tolist()
    else:
        players = []

    if not players:
        placeholder_tab2.markdown("<p style='color:gray;'>No players found in DB.</p>", unsafe_allow_html=True)
    else:
        opponents = players
        matrix_scores = pd.DataFrame("", index=players, columns=opponents)

        # ✅ Precompute dicts for fast lookup
        link_map = {
            (row.player_name, row.opponent_name): row.match_id
            for row in df_links_from_db.itertuples(index=False)
        }
        score_map = {
            (row.player_name, row.opponent_name): (row.left_score, row.right_score)
            for row in df_matches_from_db.itertuples(index=False)
        }

        # 🔁 Populate matrix
        for player in players:
            for opponent in opponents:
                if player == opponent:
                    matrix_scores.at[player, opponent] = ""
                    continue

                # ✅ optimized lookup instead of DataFrame filters
                match_id = link_map.get((player, opponent))
                if not match_id:
                    continue

                scores = score_map.get((player, opponent))
                if not scores:
                    continue

                left_score, right_score = scores

                if pd.notna(left_score) and pd.notna(right_score) and pd.notna(match_id):
                    score_text = f"{int(left_score)} : {int(right_score)}"
                    matrix_scores.at[player, opponent] = (
                        f'<a href="http://dailygammon.com/bg/game/{int(match_id)}/0/list#end" '
                        f'target="_blank">{score_text}</a>'
                    )

        # 🧱 Streamlit HTML
        html_table = matrix_scores.to_html(escape=False)
        html_table = html_table.replace('<tr><td>', '<tr><th>').replace('</td></tr>', '</th></tr>')
        html_table = html_table.replace('<table border="1" class="dataframe">', '<table class="score-matrix">')
        placeholder_tab2.markdown(html_table, unsafe_allow_html=True)

# 🔹 Build intermediate_scores from score_map (avoid re-looping df_matches_from_db)

intermediate_scores = {
    (player, opponent): (int(left_score), int(right_score))
    for (player, opponent), (left_score, right_score) in score_map.items()
    if pd.notna(left_score) and pd.notna(right_score)
}

# -----------------------
# Tab 3: Match ID Matrix (optimiert)
# -----------------------
if needs_refresh:

    # Robust extraction
    if "player_name" in df_players.columns:
        players = df_players["player_name"].astype(str).tolist()
    elif df_players.shape[1] >= 1:
        players = df_players.iloc[:, 0].astype(str).tolist()
    else:
        players = []

    # 🔹 Initialize clickable links matrix
    df_links_clickable = pd.DataFrame("", index=players, columns=players)

    # ✅ Precompute lookup dictionary
    link_map = {
        (row.player_name, row.opponent_name): row.match_id
        for row in df_links_from_db.itertuples(index=False)
    }

    # 🔁 Populate matrix using the dict (O(n²) but no DataFrame filter)
    for player in players:
        for opponent in players:
            if player == opponent:
                continue

            match_id = link_map.get((player, opponent))
            if pd.notna(match_id):
                df_links_clickable.at[player, opponent] = (
                    f'<a href="http://dailygammon.com/bg/game/{int(match_id)}/0/list#end" '
                    f'target="_blank">{int(match_id)}</a>'
                )

    # 🔹 Render HTML table in Streamlit
    html_table = df_links_clickable.to_html(escape=False)
    html_table = html_table.replace('<table border="1" class="dataframe">', '<table class="match-matrix">')
    placeholder_tab3.markdown(html_table, unsafe_allow_html=True)

else:
    pass

# -----------------------
# Build League Table / Stats for Tab 1 (Neon DB version)
# -----------------------
num_players = len(players)
total_matches_per_player = (num_players - 1) * 2  # home + away
stats = []

# intermediate_scores muss aus DB befüllt sein:
# intermediate_scores[(player, opponent)] = (player_score, opponent_score)
# Dies entspricht dem bisherigen Excel-Pull

for player in players:
    finished_matches = 0
    all_plus = all_minus = all_total = 0
    finished_plus = finished_minus = finished_total = 0

    for opponent in players:
        if player == opponent:
            continue

        # Player as row
        key_lr = (player, opponent)
        if key_lr in intermediate_scores:
            s_player, s_opponent = intermediate_scores[key_lr]
            all_plus += s_player
            all_minus += s_opponent
            all_total += s_player - s_opponent
            if s_player == 11 or s_opponent == 11:
                finished_matches += 1
                finished_plus += s_player
                finished_minus += s_opponent
                finished_total += s_player - s_opponent

        # Player as column
        key_rl = (opponent, player)
        if key_rl in intermediate_scores:
            s_opp, s_player = intermediate_scores[key_rl]
            all_plus += s_player
            all_minus += s_opp
            all_total += s_player - s_opp
            if s_player == 11 or s_opp == 11:
                finished_matches += 1
                finished_plus += s_player
                finished_minus += s_opp
                finished_total += s_player - s_opp

    # Gewonnene/Verlorene Matches
    won = sum(
        1
        for (p, o), (sp, so) in intermediate_scores.items()
        if ((p == player and sp > so) or (o == player and so > sp)) and (sp == 11 or so == 11)
    )
    lost = sum(
        1
        for (p, o), (sp, so) in intermediate_scores.items()
        if ((p == player and sp < so) or (o == player and so < sp)) and (sp == 11 or so == 11)
    )
    pct_won = round((won / (won + lost) * 100)) if (won + lost) > 0 else "---"

    stats.append([
        player,
        f"{finished_matches} / {total_matches_per_player}",
        won,
        lost,
        pct_won,
        all_plus,
        all_minus,
        all_total,
        finished_plus,
        finished_minus,
        finished_total
    ])

# --- Build DataFrame ---
df_stats = pd.DataFrame(
    stats,
    columns=[
        "Player", "Finished", "Won", "Lost", "% Won",
        "All +", "All -", "All Total",
        "Finished +", "Finished -", "Finished Total"
    ]
)

multi_cols = pd.MultiIndex.from_tuples([
    ("", "Player"),
    ("", "Finished"),
    ("", "Won"),
    ("", "Lost"),
    ("", "% Won"),
    ("All matches", "+"),
    ("All matches", "-"),
    ("All matches", "Total"),
    ("Finished matches", "+"),
    ("Finished matches", "-"),
    ("Finished matches", "Total")
])
df_stats.columns = multi_cols

# Player links aus DB verwenden
df_stats[("", "Player")] = df_stats[("", "Player")].apply(
    lambda p: f'<a href="{player_links[p]}" target="_blank">{p}</a>'
)

df_stats = df_stats.sort_values(
    by=[("", "Won"), ("Finished matches", "Total"), ("Finished matches", "+")],
    ascending=[False, False, False]
).reset_index(drop=True)

# --- Render League Table in Streamlit ---
with tab1:
    # Tabelle als HTML
    df_stats_html = df_stats.to_html(escape=False, index=False)

    # DB Last Updated (letzte echte Änderung)
    with conn.cursor() as cur:
        cur.execute("SELECT last_updated FROM groups WHERE group_id = %s;", (GROUP_ID,))
        row = cur.fetchone()
        last_changed_dt = row[0] if row and row[0] else None

    # Letztes Laden/Refresh (jetzt)
    tz = pytz.timezone("Europe/Berlin")
    last_loaded_dt = datetime.now(tz)

    # Formatieren
    last_changed_str = last_changed_dt.astimezone(tz).strftime("%b %d, %Y %H:%M %Z") if last_changed_dt else "n/a"
    last_loaded_str = last_loaded_dt.strftime("%b %d, %Y %H:%M %Z")

    # Tabelle + beide Timestamps kombinieren
    html = (
        df_stats_html +
        f"<p style='font-size:12px; color:gray;'>Last changed: {last_changed_str} | Last loaded: {last_loaded_str}</p>"
    )

    placeholder_tab1.markdown(html, unsafe_allow_html=True)
