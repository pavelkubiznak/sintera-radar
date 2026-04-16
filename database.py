"""
Datová vrstva — SQLite s exportem pro BigQuery / Looker Studio.

Schéma:
  nabidky            — každý unikátní inzerát (job_id = primární klíč z URL)
  skany              — každý scan zaznamenává datum kdy byl inzerát viděn
  konfigurace        — metadata posledního spuštění
  decision_makers    — HR decision makers z LinkedIn (per-region upload)
  dm_uploads         — log uploadů LinkedIn CSV per region
  VIEW nabidky_s_daty — nabidky + agregovaná data inzerování + odkaz na předchozí
"""

import sqlite3
import csv
import io
import json
import re
from contextlib import contextmanager
from datetime import datetime, date, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent / "data" / "jobs.db"

# ── Agency blacklist — built lazily after normalize_company is defined ────────
_AGENCY_RAW = [
    # ── Major international agencies ──
    "PŘEDVÝBĚR.CZ a.s.",
    "ManpowerGroup s.r.o.",
    "ProfesKontakt, s.r.o.",
    "Advantage Consulting, s.r.o.",
    "Jobs Contact Personal, s.r.o.",
    "Prušák Group s.r.o.",
    "mBlue Czech, s.r.o.",
    "Grafton Recruitment s.r.o.",
    "HOFMANN WIZARD s.r.o.",
    "ADECCO spol.s r.o.",
    "Randstad HR Solutions s.r.o.",
    "REED SPECIALIST RECRUITMENT",
    "Trenkwalder a.s.",
    "Axial Personnel Agency, s.r.o.",
    "JISTU recruitment s.r.o.",
    "LD Human Resources s.r.o.",
    "Manuvia a.s.",
    "Hays Czech Republic, a.s.",
    "Kelly Services Temporary Staffing",
    "Michael Page",
    "Robert Half",
    "Robert Walters",
    "Experis",
    "Devire s.r.o.",
    "Goodcall",
    "Talent Solution s.r.o.",
    "McROY Czech",
    "LMC s.r.o.",
    "YBN Consult",
    "Euro Personnel",
    "TSR Czech Republic s.r.o.",
    # ── Czech HR/staffing agencies ──
    "Talentor Advanced Search",
    "People Consulting s.r.o.",
    "Bright HR, s.r.o.",
    "ADVANCE HR,s.r.o.",
    "Brněnská personalistika, spol. s r.o.",
    "SBR Recruiting",
    "LEPŠÍ PRÁCE support s.r.o.",
    "HR PROfi",
    "HR Direct s.r.o.",
    "ISG Personalmanagement s.r.o.",
    "ISG Personalmanagement GmbH",
    "Solutions HR Specialists, s.r.o.",
    "TEMPO TRAINING & CONSULTING, a.s.",
    "CIS interim s.r.o.",
    "ETHIC HR, s.r.o.",
    "Employment Express, s.r.o.",
    "BeeClever HR s.r.o.",
    "HR Advisors CZ s.r.o.",
    "Consulta HR s.r.o.",
    "HR LEAN Solutions, s.r.o.",
    "Human Garden s.r.o.",
    "iRecruit, s.r.o.",
    "Talentem s.r.o.",
    "Talentica s.r.o.",
    "Talentinno s.r.o.",
    "Career Power, s.r.o.",
    "PersON Solutions s.r.o.",
    "Personality HR Consulting, s.r.o.",
    "Agentura Top Talent, s. r. o.",
    "PRÁCE plus, s.r.o.",
    "NonStop Consulting s.r.o.",
    "Alma Career Czechia s.r.o.",
    "ANEX personální agentura",
    "Arthur Hunt Consulting s.r.o.",
    "HOFÍREK CONSULTING",
    "AVENUE Consulting",
    "Livio consulting s.r.o.",
    "INVENT HR Consulting, s.r.o.",
    "Mardorm Consult s.r.o.",
    "BARTON Consulting s.r.o.",
    "BREDFORD Consulting, s.r.o.",
    "Neit Consulting s.r.o.",
    "OAKS Consulting s.r.o.",
    "Greyson Consulting s.r.o.",
    "Menkyna & Partners Management Consulting, s.r.o.",
    "INIZIO Internet Media s.r.o.",
    "INDEX NOSLUŠ s.r.o.",
    "Proveon, a.s.",
    "DRILL Business Services",
    "GHS Consulting s.r.o.",
    "ŽOLÍKOVÁ PRÁCE s.r.o.",
    "stálýNÁJEM.CZ",
    "Swiss Life Select",
]

AGENCY_KEYWORDS = [
    # English patterns
    "recruitment", "recruiting", "personnel", "staffing",
    "headhunt", "manpower", "adecco", "randstad", "grafton",
    "trenkwalder", "hofmann wizard", "hays ",
    "kelly services", "michael page", "robert half",
    "robert walters", "předvýběr", "profeskontakt", "manuvia",
    "jistu", "axial personnel",
    # Czech HR/staffing patterns
    "personální", "personalistik", "personalmanagement",
    # Targeted company-word patterns (avoids false positives with cities)
    "hr consulting", "hr specialist", "hr solution", "hr advisor",
    "hr direct", "hr lean", "hr profi",
    "bright hr", "advance hr", "ethic hr", "beeclever hr",
    "consulta hr",
    # Staffing/recruitment specific
    "talentor", "irecruit", "výběrovka",
    "lepší práce", "žolíkov",
    "employment express",
]

_agency_norms: set = set()  # populated on first call to is_agency()


def is_agency(firma: str) -> bool:
    """Check if a company name matches known agency patterns."""
    global _agency_norms
    if not firma:
        return False
    # Lazy init: build normalized set on first call
    if not _agency_norms:
        _agency_norms = {normalize_company(r) for r in _AGENCY_RAW}
    norm = normalize_company(firma)
    if norm in _agency_norms:
        return True
    lower = firma.lower()
    for kw in AGENCY_KEYWORDS:
        if kw in lower:
            return True
    return False


def _mark_agencies(conn):
    """Re-scan all firms and mark/unmark agencies based on current blacklist.
    Runs on every init_db() to catch newly added agency patterns.
    """
    firms = conn.execute("SELECT DISTINCT firma FROM nabidky WHERE firma != ''").fetchall()
    agency_firms = [r[0] for r in firms if is_agency(r[0])]
    if agency_firms:
        placeholders = ",".join("?" * len(agency_firms))
        conn.execute(
            f"UPDATE nabidky SET is_agency = 1 WHERE firma IN ({placeholders})",
            agency_firms,
        )
        print(f"[init_db] Marked {len(agency_firms)} agency companies as is_agency=1")


def init_db() -> None:
    """Vytvoří databázi, tabulky a view. Bezpečně migruje existující DB."""
    DB_PATH.parent.mkdir(exist_ok=True)
    with get_conn() as conn:
        # Hlavní tabulky
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS nabidky (
                job_id              TEXT PRIMARY KEY,
                pozice              TEXT NOT NULL,
                firma               TEXT,
                misto               TEXT,
                kraj                TEXT,
                kraj_slug           TEXT,
                plat_text           TEXT,
                plat_od             INTEGER,
                plat_do             INTEGER,
                url                 TEXT,
                datum_prvni_scan    TEXT NOT NULL,
                datum_posledni_scan TEXT NOT NULL,
                pocet_scanu         INTEGER DEFAULT 1,
                aktivni             INTEGER DEFAULT 1,
                predchozi_job_id    TEXT,
                predchozi_datum_prvni TEXT
            );

            CREATE TABLE IF NOT EXISTS skany (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id      TEXT NOT NULL,
                datum_scan  TEXT NOT NULL,
                kraj        TEXT,
                FOREIGN KEY (job_id) REFERENCES nabidky(job_id)
            );

            CREATE TABLE IF NOT EXISTS konfigurace (
                klic    TEXT PRIMARY KEY,
                hodnota TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_nabidky_firma       ON nabidky(firma);
            CREATE INDEX IF NOT EXISTS idx_nabidky_kraj        ON nabidky(kraj);
            CREATE INDEX IF NOT EXISTS idx_nabidky_datum_prvni ON nabidky(datum_prvni_scan);
            CREATE INDEX IF NOT EXISTS idx_nabidky_firma_pozice ON nabidky(firma, pozice);
            CREATE INDEX IF NOT EXISTS idx_skany_job_id        ON skany(job_id);
            CREATE INDEX IF NOT EXISTS idx_skany_datum         ON skany(datum_scan);

            -- Decision makers (LinkedIn)
            CREATE TABLE IF NOT EXISTS decision_makers (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name      TEXT,
                last_name       TEXT,
                full_name       TEXT,
                headline        TEXT,
                location        TEXT,
                current_title   TEXT,
                current_company TEXT,
                company_normalized TEXT,
                email           TEXT,
                phone           TEXT,
                profile_url     TEXT UNIQUE,
                kraj            TEXT,
                notes           TEXT,
                datum_upload     TEXT,
                datum_aktualizace TEXT
            );

            CREATE TABLE IF NOT EXISTS dm_uploads (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                kraj        TEXT NOT NULL,
                pocet       INTEGER,
                datum       TEXT NOT NULL,
                soubor      TEXT
            );

            -- Company alias table for manual linking
            CREATE TABLE IF NOT EXISTS company_aliases (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                firma_jobs      TEXT NOT NULL,
                firma_linkedin  TEXT NOT NULL,
                datum           TEXT NOT NULL
            );

            -- Outreach CRM
            CREATE TABLE IF NOT EXISTS outreach (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                firma           TEXT NOT NULL,
                firma_norm      TEXT NOT NULL,
                kontakt_jmeno   TEXT,
                kontakt_url     TEXT,
                kanal           TEXT DEFAULT 'LinkedIn DM',
                status          TEXT DEFAULT 'odeslano',
                poznamka        TEXT DEFAULT '',
                datum_osloveni  TEXT NOT NULL,
                datum_aktualizace TEXT,
                kraj            TEXT
            );

            -- Scrape log for anomaly detection
            CREATE TABLE IF NOT EXISTS scrape_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                datum       TEXT NOT NULL,
                den_tydne   INTEGER NOT NULL,
                kraj        TEXT NOT NULL,
                pocet       INTEGER NOT NULL DEFAULT 0,
                novych      INTEGER NOT NULL DEFAULT 0,
                chyba       TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_scrape_log_datum ON scrape_log(datum);

            CREATE INDEX IF NOT EXISTS idx_dm_company ON decision_makers(company_normalized);
            CREATE INDEX IF NOT EXISTS idx_dm_kraj    ON decision_makers(kraj);
            CREATE INDEX IF NOT EXISTS idx_dm_profile ON decision_makers(profile_url);
            CREATE INDEX IF NOT EXISTS idx_alias_jobs ON company_aliases(firma_jobs);
            CREATE INDEX IF NOT EXISTS idx_alias_li   ON company_aliases(firma_linkedin);
            CREATE INDEX IF NOT EXISTS idx_outreach_firma ON outreach(firma_norm);
            CREATE INDEX IF NOT EXISTS idx_outreach_status ON outreach(status);
        """)

        # Migrace: přidej nové sloupce do existující DB pokud chybí
        existing_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(nabidky)").fetchall()
        }
        migrations = {
            "predchozi_job_id":     "ALTER TABLE nabidky ADD COLUMN predchozi_job_id TEXT",
            "predchozi_datum_prvni":"ALTER TABLE nabidky ADD COLUMN predchozi_datum_prvni TEXT",
            "obor":                 "ALTER TABLE nabidky ADD COLUMN obor TEXT DEFAULT ''",
            "datum_vydani":         "ALTER TABLE nabidky ADD COLUMN datum_vydani TEXT DEFAULT ''",
            "publikovano":          "ALTER TABLE nabidky ADD COLUMN publikovano TEXT DEFAULT ''",
            "is_agency":            "ALTER TABLE nabidky ADD COLUMN is_agency INTEGER DEFAULT 0",
        }
        for col, sql in migrations.items():
            if col not in existing_cols:
                conn.execute(sql)

        # Always re-scan agencies (catches newly added patterns)
        _mark_agencies(conn)

        # Index for agency filter (safe to create after migration)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_nabidky_agency ON nabidky(is_agency)")

        # VIEW: nabidky + seznam dat inzerování
        conn.executescript("""
            DROP VIEW IF EXISTS nabidky_s_daty;
            CREATE VIEW nabidky_s_daty AS
            SELECT
                n.job_id,
                n.pozice,
                n.firma,
                n.misto,
                n.kraj,
                n.obor,
                n.datum_vydani,
                n.plat_text,
                n.plat_od,
                n.plat_do,
                n.url,
                n.pocet_scanu,
                n.aktivni,
                n.is_agency,
                n.publikovano,
                n.datum_prvni_scan,
                n.datum_posledni_scan,
                n.predchozi_job_id,
                n.predchozi_datum_prvni,
                -- Počet dní aktivní inzerce
                CAST(
                    (JULIANDAY(n.datum_posledni_scan) - JULIANDAY(n.datum_prvni_scan))
                    AS INTEGER
                ) AS dni_aktivni,
                -- Všechna data skenování jako čárkou oddělený seznam
                (
                    SELECT GROUP_CONCAT(DISTINCT s.datum_scan ORDER BY s.datum_scan)
                    FROM skany s
                    WHERE s.job_id = n.job_id
                ) AS data_inzerovani,
                -- Příznak opakovaného zadání
                CASE WHEN n.predchozi_job_id IS NOT NULL THEN 1 ELSE 0 END AS je_opakovani
            FROM nabidky n;
        """)


@contextmanager
def get_conn():
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Pomocné funkce ────────────────────────────────────────────────────────────

def _parse_plat(plat_text: str) -> tuple:
    """'45 000 – 55 000 Kč' → (45000, 55000)"""
    if not plat_text:
        return None, None
    clean = plat_text.replace("\u200d", "").replace("\xa0", " ")
    nums = re.findall(r"[\d\s]+", clean)
    nums = [int(n.replace(" ", "")) for n in nums if n.strip()]
    if not nums:
        return None, None
    return (nums[0], nums[0]) if len(nums) == 1 else (nums[0], nums[1])


def _najdi_predchozi(conn, firma: str, pozice: str) -> Optional[tuple]:
    """
    Hledá neaktivní záznam stejné firmy a stejné pozice zaniklý max. 180 dní zpět.
    Vrací (job_id, datum_prvni_scan) nebo None.
    Porovnání: přesná shoda firma + pozice (case-insensitive, bez mezer).
    """
    if not firma or not pozice:
        return None
    hranice = (date.today() - timedelta(days=180)).isoformat()
    row = conn.execute("""
        SELECT job_id, datum_prvni_scan
        FROM nabidky
        WHERE aktivni = 0
          AND LOWER(TRIM(firma))  = LOWER(TRIM(?))
          AND LOWER(TRIM(pozice)) = LOWER(TRIM(?))
          AND datum_posledni_scan >= ?
        ORDER BY datum_posledni_scan DESC
        LIMIT 1
    """, (firma, pozice, hranice)).fetchone()
    return (row["job_id"], row["datum_prvni_scan"]) if row else None


# ── Zápis do DB ───────────────────────────────────────────────────────────────

def uloz_nabidky(nabidky: list) -> dict:
    """
    Uloží seznam nabídek do databáze.
    Vrací statistiku: {nove, aktualizovane, opakovani, celkem}.
    """
    if not nabidky:
        return {"nove": 0, "aktualizovane": 0, "opakovani": 0, "celkem": 0}

    now   = datetime.now().isoformat(timespec="seconds")
    dnes  = date.today().isoformat()
    nove  = 0
    aktualizovane = 0
    opakovani = 0

    with get_conn() as conn:
        # Označíme jako neaktivní vše v dotčených krajích
        kraje = {n["kraj"] for n in nabidky if n.get("kraj")}
        for kraj in kraje:
            conn.execute("UPDATE nabidky SET aktivni = 0 WHERE kraj = ?", (kraj,))

        for n in nabidky:
            job_id = n.get("job_id")
            if not job_id:
                continue

            plat_od, plat_do = _parse_plat(n.get("plat", ""))

            existing = conn.execute(
                "SELECT job_id FROM nabidky WHERE job_id = ?", (job_id,)
            ).fetchone()

            if existing:
                # Only increment pocet_scanu once per day (prevent inflation
                # when same job appears in multiple obor passes)
                already_today = conn.execute(
                    "SELECT 1 FROM skany WHERE job_id = ? AND datum_scan = ?",
                    (job_id, dnes)
                ).fetchone()
                increment = "pocet_scanu + 1" if not already_today else "pocet_scanu"
                conn.execute("""
                    UPDATE nabidky SET
                        datum_posledni_scan = ?,
                        pocet_scanu = {},
                        aktivni = 1,
                        plat_text = COALESCE(NULLIF(?, ''), plat_text),
                        plat_od   = COALESCE(?, plat_od),
                        plat_do   = COALESCE(?, plat_do),
                        obor      = COALESCE(NULLIF(?, ''), obor),
                        datum_vydani = COALESCE(NULLIF(?, ''), datum_vydani),
                        publikovano  = COALESCE(NULLIF(?, ''), publikovano)
                    WHERE job_id = ?
                """.format(increment), (now, n.get("plat", ""), plat_od, plat_do,
                      n.get("obor", ""), n.get("datum_vydani", ""),
                      n.get("publikovano", ""), job_id))
                aktualizovane += 1
            else:
                # Hledáme předchozí zadání stejné firmy + pozice
                predchozi = _najdi_predchozi(
                    conn, n.get("firma", ""), n.get("pozice", "")
                )
                predchozi_job_id    = predchozi[0] if predchozi else None
                predchozi_datum     = predchozi[1] if predchozi else None
                if predchozi:
                    opakovani += 1

                agency_flag = 1 if is_agency(n.get("firma", "")) else 0
                conn.execute("""
                    INSERT INTO nabidky
                        (job_id, pozice, firma, misto, kraj, kraj_slug,
                         plat_text, plat_od, plat_do, url, obor, datum_vydani,
                         publikovano,
                         datum_prvni_scan, datum_posledni_scan,
                         pocet_scanu, aktivni,
                         predchozi_job_id, predchozi_datum_prvni, is_agency)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?)
                """, (
                    job_id,
                    n.get("pozice", ""),
                    n.get("firma", ""),
                    n.get("misto", ""),
                    n.get("kraj", ""),
                    n.get("kraj_slug", ""),
                    n.get("plat", ""),
                    plat_od, plat_do,
                    n.get("url", ""),
                    n.get("obor", ""),
                    n.get("datum_vydani", ""),
                    n.get("publikovano", ""),
                    now, now,
                    predchozi_job_id, predchozi_datum, agency_flag,
                ))
                nove += 1

            # Scan záznam — pouze jednou za den na job_id
            already = conn.execute(
                "SELECT 1 FROM skany WHERE job_id = ? AND datum_scan = ?",
                (job_id, dnes)
            ).fetchone()
            if not already:
                conn.execute(
                    "INSERT INTO skany (job_id, datum_scan, kraj) VALUES (?, ?, ?)",
                    (job_id, dnes, n.get("kraj", ""))
                )

        conn.execute(
            "INSERT OR REPLACE INTO konfigurace (klic, hodnota) VALUES ('posledni_scan', ?)",
            (now,)
        )

    return {
        "nove": nove, "aktualizovane": aktualizovane,
        "opakovani": opakovani,
        "celkem": nove + aktualizovane,
    }


# ── Čtení ─────────────────────────────────────────────────────────────────────

def nacti_nabidky(
    kraj: str = "",
    aktivni_pouze: bool = False,
    min_scanu: int = 0,
    limit: int = 100,
    offset: int = 0,
) -> list:
    with get_conn() as conn:
        where, params = ["is_agency = 0"], []
        if kraj:
            where.append("kraj = ?"); params.append(kraj)
        if aktivni_pouze:
            where.append("aktivni = 1")
        if min_scanu > 0:
            where.append("pocet_scanu >= ?"); params.append(min_scanu)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        params += [limit, offset]
        return conn.execute(
            f"""SELECT * FROM nabidky_s_daty {clause}
                ORDER BY pocet_scanu DESC, datum_posledni_scan DESC
                LIMIT ? OFFSET ?""",
            params,
        ).fetchall()


def statistiky() -> dict:
    # Agency filter applied to all queries
    na = "AND is_agency = 0"
    with get_conn() as conn:
        total       = conn.execute("SELECT COUNT(*) FROM nabidky WHERE is_agency = 0").fetchone()[0]
        aktivni     = conn.execute(f"SELECT COUNT(*) FROM nabidky WHERE aktivni = 1 {na}").fetchone()[0]
        nove_dnes   = conn.execute(
            f"SELECT COUNT(*) FROM nabidky WHERE DATE(datum_prvni_scan) = DATE('now') {na}"
        ).fetchone()[0]
        opakovani_celkem = conn.execute(
            f"SELECT COUNT(*) FROM nabidky WHERE predchozi_job_id IS NOT NULL AND aktivni = 1 {na}"
        ).fetchone()[0]
        top_firmy   = conn.execute(f"""
            SELECT firma, COUNT(*) as pocet_inzeratu, MAX(pocet_scanu) as max_scanu
            FROM nabidky WHERE firma != '' AND aktivni = 1 {na}
            GROUP BY firma ORDER BY pocet_inzeratu DESC LIMIT 10
        """).fetchall()
        dlouho_aktivni_count = conn.execute(f"""
            SELECT COUNT(*) FROM nabidky WHERE aktivni = 1 AND pocet_scanu >= 4 {na}
        """).fetchone()[0]
        dlouho_aktivni = conn.execute(f"""
            SELECT * FROM nabidky WHERE aktivni = 1 AND pocet_scanu >= 4 {na}
            ORDER BY pocet_scanu DESC LIMIT 20
        """).fetchall()
        posledni_scan = conn.execute(
            "SELECT hodnota FROM konfigurace WHERE klic = 'posledni_scan'"
        ).fetchone()
        kraje_stats = conn.execute(f"""
            SELECT kraj, COUNT(*) as pocet FROM nabidky WHERE aktivni = 1 {na}
            GROUP BY kraj ORDER BY pocet DESC
        """).fetchall()
        opakovane_pozice = conn.execute(f"""
            SELECT n.firma, n.pozice, n.kraj,
                   n.datum_prvni_scan, n.predchozi_datum_prvni,
                   n.url, n.job_id
            FROM nabidky n
            WHERE n.predchozi_job_id IS NOT NULL AND n.aktivni = 1 {na}
            ORDER BY n.datum_prvni_scan DESC LIMIT 15
        """).fetchall()

    return {
        "total": total,
        "aktivni": aktivni,
        "nove_dnes": nove_dnes,
        "opakovani_celkem": opakovani_celkem,
        "dlouho_aktivni_count": dlouho_aktivni_count,
        "top_firmy": [dict(r) for r in top_firmy],
        "dlouho_aktivni": [dict(r) for r in dlouho_aktivni],
        "posledni_scan": posledni_scan[0] if posledni_scan else "—",
        "kraje_stats": [dict(r) for r in kraje_stats],
        "opakovane_pozice": [dict(r) for r in opakovane_pozice],
    }


def exportuj_csv(soubor: str = "data/export.csv") -> str:
    """Exportuje VIEW nabidky_s_daty do CSV — připraveno pro Looker Studio / Power BI."""
    path = Path(soubor)
    path.parent.mkdir(exist_ok=True)
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM nabidky_s_daty ORDER BY datum_prvni_scan DESC"
        ).fetchall()
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows([dict(r) for r in rows])
    return str(path)


# ── Normalizace firemních jmen ────────────────────────────────────────────────

_LEGAL_FORMS = re.compile(
    r"\b(s\.?\s*r\.?\s*o\.?|a\.?\s*s\.?|spol\.?\s*s\s*r\.?\s*o\.?|"
    r"v\.?\s*o\.?\s*s\.?|k\.?\s*s\.?|s\.?\s*e\.?|"
    r"gmbh|ltd|inc|corp|plc|ag|group|holding)\b",
    re.IGNORECASE,
)
_WHITESPACE = re.compile(r"\s+")


def normalize_company(name: str) -> str:
    """Normalizuje firemní jméno pro porovnávání.
    Odstraní právní formu, interpunkci, převede na lowercase."""
    if not name:
        return ""
    s = name.lower().strip()
    s = _LEGAL_FORMS.sub("", s)
    s = re.sub(r"[,.\-–—/&()\"']", " ", s)
    s = _WHITESPACE.sub(" ", s).strip()
    return s


def _match_score(a: str, b: str) -> float:
    """Vrací similaritu dvou normalizovaných názvů (0.0 – 1.0)."""
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    # Zkusíme jestli jeden obsahuje druhý (substrings)
    if a in b or b in a:
        return 0.92
    return SequenceMatcher(None, a, b).ratio()


# ── LinkedIn CSV import ──────────────────────────────────────────────────────

def import_linkedin_csv(csv_content: str, kraj: str) -> dict:
    """Importuje LinkedIn pipeline CSV do decision_makers.
    Nahradí všechny záznamy daného kraje (per-region replace).
    Vrací {imported, skipped, total}.
    """
    now = datetime.now().isoformat(timespec="seconds")
    reader = csv.DictReader(io.StringIO(csv_content))

    rows = []
    for row in reader:
        profile_url = (row.get("Profile URL") or "").strip()
        if not profile_url:
            continue
        first = (row.get("First Name") or "").strip()
        last = (row.get("Last Name") or "").strip()
        company = (row.get("Current Company") or "").strip()
        rows.append({
            "first_name": first,
            "last_name": last,
            "full_name": "{} {}".format(first, last).strip(),
            "headline": (row.get("Headline") or "").strip(),
            "location": (row.get("Location") or "").strip(),
            "current_title": (row.get("Current Title") or "").strip(),
            "current_company": company,
            "company_normalized": normalize_company(company),
            "email": (row.get("Email Address") or "").strip(),
            "phone": (row.get("Phone Number") or "").strip(),
            "profile_url": profile_url,
            "kraj": kraj,
            "notes": (row.get("Notes") or "").strip(),
            "datum_upload": now,
            "datum_aktualizace": now,
        })

    if not rows:
        return {"imported": 0, "skipped": 0, "total": 0}

    imported = 0
    skipped = 0

    with get_conn() as conn:
        # Delete existing records for this region
        conn.execute("DELETE FROM decision_makers WHERE kraj = ?", (kraj,))

        for r in rows:
            # Check for duplicate profile_url from other regions
            existing = conn.execute(
                "SELECT id FROM decision_makers WHERE profile_url = ?",
                (r["profile_url"],)
            ).fetchone()
            if existing:
                # Update existing record (person might be in multiple pipelines)
                conn.execute("""
                    UPDATE decision_makers SET
                        first_name = ?, last_name = ?, full_name = ?,
                        headline = ?, location = ?, current_title = ?,
                        current_company = ?, company_normalized = ?,
                        email = COALESCE(NULLIF(?, ''), email),
                        phone = COALESCE(NULLIF(?, ''), phone),
                        notes = ?, datum_aktualizace = ?
                    WHERE profile_url = ?
                """, (r["first_name"], r["last_name"], r["full_name"],
                      r["headline"], r["location"], r["current_title"],
                      r["current_company"], r["company_normalized"],
                      r["email"], r["phone"], r["notes"], now,
                      r["profile_url"]))
                skipped += 1
            else:
                conn.execute("""
                    INSERT INTO decision_makers
                        (first_name, last_name, full_name, headline, location,
                         current_title, current_company, company_normalized,
                         email, phone, profile_url, kraj, notes,
                         datum_upload, datum_aktualizace)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (r["first_name"], r["last_name"], r["full_name"],
                      r["headline"], r["location"], r["current_title"],
                      r["current_company"], r["company_normalized"],
                      r["email"], r["phone"], r["profile_url"],
                      r["kraj"], r["notes"], now, now))
                imported += 1

        # Log the upload
        conn.execute(
            "INSERT INTO dm_uploads (kraj, pocet, datum, soubor) VALUES (?, ?, ?, ?)",
            (kraj, imported + skipped, now, "batch upload")
        )

    return {"imported": imported, "skipped": skipped, "total": imported + skipped}


def add_decision_maker(data: dict) -> bool:
    """Přidá jednoho decision makera ručně."""
    now = datetime.now().isoformat(timespec="seconds")
    company = data.get("current_company", "")
    profile_url = data.get("profile_url", "").strip()
    if not profile_url:
        return False

    first = data.get("first_name", "").strip()
    last = data.get("last_name", "").strip()

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT id FROM decision_makers WHERE profile_url = ?",
            (profile_url,)
        ).fetchone()
        if existing:
            conn.execute("""
                UPDATE decision_makers SET
                    first_name = ?, last_name = ?, full_name = ?,
                    current_title = ?, current_company = ?, company_normalized = ?,
                    kraj = ?, datum_aktualizace = ?
                WHERE profile_url = ?
            """, (first, last, "{} {}".format(first, last).strip(),
                  data.get("current_title", ""), company,
                  normalize_company(company),
                  data.get("kraj", ""), now, profile_url))
        else:
            conn.execute("""
                INSERT INTO decision_makers
                    (first_name, last_name, full_name, headline, location,
                     current_title, current_company, company_normalized,
                     email, phone, profile_url, kraj, notes,
                     datum_upload, datum_aktualizace)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (first, last, "{} {}".format(first, last).strip(),
                  data.get("headline", ""), data.get("location", ""),
                  data.get("current_title", ""), company,
                  normalize_company(company),
                  data.get("email", ""), data.get("phone", ""),
                  profile_url, data.get("kraj", ""),
                  data.get("notes", ""), now, now))
    return True


# ── Decision maker stats ─────────────────────────────────────────────────────

def dm_statistiky() -> dict:
    """Statistiky decision makerů."""
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM decision_makers").fetchone()[0]
        per_kraj = conn.execute("""
            SELECT kraj, COUNT(*) as pocet FROM decision_makers
            GROUP BY kraj ORDER BY pocet DESC
        """).fetchall()
        uploads = conn.execute("""
            SELECT kraj, pocet, datum FROM dm_uploads
            ORDER BY datum DESC LIMIT 20
        """).fetchall()
        top_companies = conn.execute("""
            SELECT current_company, COUNT(*) as pocet
            FROM decision_makers WHERE current_company != ''
            GROUP BY company_normalized ORDER BY pocet DESC LIMIT 15
        """).fetchall()
    return {
        "total": total,
        "per_kraj": [dict(r) for r in per_kraj],
        "uploads": [dict(r) for r in uploads],
        "top_companies": [dict(r) for r in top_companies],
    }


def nacti_decision_makers(kraj: str = "", limit: int = 100, offset: int = 0) -> list:
    with get_conn() as conn:
        where, params = [], []
        if kraj:
            where.append("kraj = ?")
            params.append(kraj)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        params += [limit, offset]
        return [dict(r) for r in conn.execute(
            f"""SELECT * FROM decision_makers {clause}
                ORDER BY current_company, last_name
                LIMIT ? OFFSET ?""",
            params
        ).fetchall()]


# ── RADAR: Matching & Intelligence ───────────────────────────────────────────

def _load_manual_aliases() -> dict:
    """Načte manuální propojení firma_jobs → firma_linkedin."""
    with get_conn() as conn:
        rows = conn.execute("SELECT firma_jobs, firma_linkedin FROM company_aliases").fetchall()
    aliases = {}
    for r in rows:
        aliases[normalize_company(r["firma_jobs"])] = normalize_company(r["firma_linkedin"])
    return aliases


def add_company_alias(firma_jobs: str, firma_linkedin: str) -> None:
    """Přidá manuální propojení dvou firemních jmen."""
    now = datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        # Check if already exists
        existing = conn.execute(
            "SELECT id FROM company_aliases WHERE firma_jobs = ? AND firma_linkedin = ?",
            (firma_jobs, firma_linkedin)
        ).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO company_aliases (firma_jobs, firma_linkedin, datum) VALUES (?, ?, ?)",
                (firma_jobs, firma_linkedin, now)
            )


def radar_matches(min_scanu: int = 1, min_score: float = 0.82, skip_fuzzy: bool = False) -> list:
    """Hlavní funkce radaru.
    Najde firmy z jobs.cz, které mají odpovídajícího decision makera.
    Vrací seznam matchů seřazený podle signálu (počet pozic, délka inzerce).

    Optimalizováno pro rychlost: exact match a substring match jsou O(1) per firma.
    Fuzzy match (pomalý) se spouští jen na firmy s 3+ pozicemi, které nebyly nalezeny jinak.
    skip_fuzzy=True skips the expensive fuzzy pass (for dashboard/fast views).
    """
    aliases = _load_manual_aliases()

    with get_conn() as conn:
        # All active jobs, grouped by normalized company name
        jobs_by_company = {}
        job_rows = conn.execute("""
            SELECT job_id, pozice, firma, kraj, obor, plat_text, url,
                   pocet_scanu, datum_prvni_scan, datum_posledni_scan,
                   datum_vydani, aktivni,
                   predchozi_job_id, predchozi_datum_prvni
            FROM nabidky
            WHERE aktivni = 1 AND firma != '' AND is_agency = 0
            ORDER BY firma, pocet_scanu DESC
        """).fetchall()

        for j in job_rows:
            norm = normalize_company(j["firma"])
            if norm not in jobs_by_company:
                jobs_by_company[norm] = {
                    "firma": j["firma"],
                    "firma_norm": norm,
                    "pozice": [],
                }
            jobs_by_company[norm]["pozice"].append(dict(j))

        # All decision makers, grouped by normalized company name
        dm_by_company = {}
        dm_rows = conn.execute("""
            SELECT * FROM decision_makers
            WHERE current_company != ''
        """).fetchall()

        for dm in dm_rows:
            norm = dm["company_normalized"]
            if norm not in dm_by_company:
                dm_by_company[norm] = []
            dm_by_company[norm].append(dict(dm))

    # Pre-build substring index for fast lookup
    dm_norms = list(dm_by_company.keys())

    # Match: jobs company → DM company (3 passes: alias → exact → substring → fuzzy)
    matches = []
    matched_job_norms = set()

    for job_norm, job_data in jobs_by_company.items():
        best_dm_norm = None
        best_score = 0.0
        match_type = ""

        # 1. Manual alias (O(1))
        if job_norm in aliases:
            alias_norm = aliases[job_norm]
            if alias_norm in dm_by_company:
                best_dm_norm = alias_norm
                best_score = 1.0
                match_type = "manual"

        # 2. Exact match (O(1))
        if not best_dm_norm and job_norm in dm_by_company:
            best_dm_norm = job_norm
            best_score = 1.0
            match_type = "exact"

        # 3. Substring match (O(dm_count) but very fast string ops)
        if not best_dm_norm and len(job_norm) >= 4:
            for dm_norm in dm_norms:
                if len(dm_norm) >= 4 and (job_norm in dm_norm or dm_norm in job_norm):
                    best_dm_norm = dm_norm
                    best_score = 0.92
                    match_type = "substring"
                    break

        if best_dm_norm and best_dm_norm in dm_by_company:
            pozice_list = job_data["pozice"]
            max_scanu = max(p["pocet_scanu"] for p in pozice_list)
            has_long_running = any(p["pocet_scanu"] >= 4 for p in pozice_list)
            has_repeated = any(p.get("predchozi_job_id") for p in pozice_list)

            # Signal scoring
            signal_score = len(pozice_list) * 2  # more positions = higher
            signal_score += max_scanu  # longer running = higher
            if has_long_running:
                signal_score += 10
            if has_repeated:
                signal_score += 15  # repeated = chronic problem

            # Signal label
            if has_repeated:
                signal = "Opakovaně neobsazují"
            elif has_long_running:
                signal = "Dlouhodobě hledají"
            elif len(pozice_list) >= 3:
                signal = "Rozšiřují tým"
            else:
                signal = "Aktivní nábor"

            kraje_set = set()
            for p in pozice_list:
                if p.get("kraj"):
                    kraje_set.add(p["kraj"])

            matches.append({
                "firma": job_data["firma"],
                "firma_norm": job_norm,
                "pozice": pozice_list,
                "pocet_pozic": len(pozice_list),
                "max_scanu": max_scanu,
                "decision_makers": dm_by_company[best_dm_norm],
                "match_score": best_score,
                "match_type": match_type,
                "signal_score": signal_score,
                "signal": signal,
                "kraje": sorted(kraje_set),
            })
            matched_job_norms.add(job_norm)

    # 4. Fuzzy pass — ONLY for unmatched companies with 3+ positions (worth the cost)
    #    Skipped when skip_fuzzy=True (dashboard, recommendations) to avoid O(N*M) hang.
    unmatched_valuable = [
        (norm, data) for norm, data in jobs_by_company.items()
        if norm not in matched_job_norms and len(data["pozice"]) >= 3
    ]
    if unmatched_valuable and dm_norms and not skip_fuzzy:
        for job_norm, job_data in unmatched_valuable:
            best_dm_norm = None
            best_score = 0.0
            for dm_norm in dm_norms:
                score = _match_score(job_norm, dm_norm)
                if score > best_score and score >= min_score:
                    best_score = score
                    best_dm_norm = dm_norm
            if best_dm_norm:
                pozice_list = job_data["pozice"]
                max_scanu = max(p["pocet_scanu"] for p in pozice_list)
                has_long_running = any(p["pocet_scanu"] >= 4 for p in pozice_list)
                has_repeated = any(p.get("predchozi_job_id") for p in pozice_list)
                signal_score = len(pozice_list) * 2 + max_scanu
                if has_long_running:
                    signal_score += 10
                if has_repeated:
                    signal_score += 15
                if has_repeated:
                    signal = "Opakovaně neobsazují"
                elif has_long_running:
                    signal = "Dlouhodobě hledají"
                elif len(pozice_list) >= 3:
                    signal = "Rozšiřují tým"
                else:
                    signal = "Aktivní nábor"
                kraje_set = set()
                for p in pozice_list:
                    if p.get("kraj"):
                        kraje_set.add(p["kraj"])
                matches.append({
                    "firma": job_data["firma"],
                    "firma_norm": job_norm,
                    "pozice": pozice_list,
                    "pocet_pozic": len(pozice_list),
                    "max_scanu": max_scanu,
                    "decision_makers": dm_by_company[best_dm_norm],
                    "match_score": best_score,
                    "match_type": "fuzzy",
                    "signal_score": signal_score,
                    "signal": signal,
                    "kraje": sorted(kraje_set),
                })
                matched_job_norms.add(job_norm)

    # Enrich with outreach status
    with get_conn() as conn:
        outreach_rows = conn.execute(
            "SELECT firma_norm, status, datum_osloveni FROM outreach ORDER BY datum_osloveni DESC"
        ).fetchall()
    outreach_map = {}
    for o in outreach_rows:
        if o["firma_norm"] not in outreach_map:
            outreach_map[o["firma_norm"]] = o["status"]

    for m in matches:
        m["outreach_status"] = outreach_map.get(m["firma_norm"], "")

    # Sort by signal score descending
    matches.sort(key=lambda m: m["signal_score"], reverse=True)

    # Stats
    total_jobs_companies = len(jobs_by_company)
    total_dm_companies = len(dm_by_company)
    matched_count = len(matched_job_norms)
    contacted = sum(1 for m in matches if m["outreach_status"])

    return {
        "matches": matches,
        "stats": {
            "total_jobs_companies": total_jobs_companies,
            "total_dm_companies": total_dm_companies,
            "matched": matched_count,
            "unmatched_jobs": total_jobs_companies - matched_count,
            "contacted": contacted,
        },
    }


def outreach_brief(firma_norm: str) -> Optional[dict]:
    """Generuje strukturovaný brief pro jednu firmu (pro ChatGPT outreach)."""
    result = radar_matches(skip_fuzzy=True)
    for m in result["matches"]:
        if m["firma_norm"] == firma_norm:
            pozice_lines = []
            for p in m["pozice"]:
                weeks = max(1, p["pocet_scanu"])
                line = "{} ({} týdnů)".format(p["pozice"], weeks)
                if p.get("predchozi_job_id"):
                    line += " — OPAKOVANĚ"
                if p.get("plat_text"):
                    line += " — {}".format(p["plat_text"])
                pozice_lines.append(line)

            dm_lines = []
            for dm in m["decision_makers"]:
                dm_lines.append("{} — {} — {}".format(
                    dm["full_name"], dm["current_title"], dm["profile_url"]
                ))

            return {
                "firma": m["firma"],
                "kraje": ", ".join(m["kraje"]),
                "signal": m["signal"],
                "pozice": pozice_lines,
                "decision_makers": dm_lines,
                "pocet_pozic": m["pocet_pozic"],
                "max_scanu": m["max_scanu"],
            }
    return None


# ── Company detail ────────────────────────────────────────────────────────────

def firma_detail(firma_norm: str) -> Optional[dict]:
    """Kompletní přehled o firmě — pozice, kontakty, outreach historie.
    Each position is enriched with flags and computed fields.
    """
    with get_conn() as conn:
        # Find the original firma name(s) matching this normalized form
        all_firms = conn.execute(
            "SELECT DISTINCT firma FROM nabidky WHERE firma != ''"
        ).fetchall()

        matching_firms = []
        for row in all_firms:
            if normalize_company(row["firma"]) == firma_norm:
                matching_firms.append(row["firma"])

        if not matching_firms:
            return None

        target_firma = matching_firms[0]

        # Get all positions for matching firm names
        placeholders = ",".join("?" * len(matching_firms))
        pozice = [dict(r) for r in conn.execute(f"""
            SELECT job_id, pozice, kraj, obor, plat_text, url,
                   pocet_scanu, datum_prvni_scan, datum_posledni_scan,
                   datum_vydani, publikovano, aktivni,
                   predchozi_job_id, predchozi_datum_prvni,
                   CAST(JULIANDAY('now') - JULIANDAY(datum_prvni_scan) AS INTEGER) AS dni_aktivni
            FROM nabidky
            WHERE firma IN ({placeholders})
            ORDER BY aktivni DESC, pocet_scanu DESC, datum_prvni_scan DESC
        """, matching_firms).fetchall()]

        # Enrich each position with flags
        for p in pozice:
            flags = []
            if p.get("publikovano") and "ktualizov" in p["publikovano"]:
                flags.append({
                    "typ": "aktualizovano",
                    "barva": "#dc2626",
                    "bg": "#fef2f2",
                    "text": "Aktualizovano",
                    "duvod": "Firma tuto pozici obnovila na jobs.cz. To znamena, ze ji drive vypsali, neobsadili, a ted se snazi znovu. Silny signal ze potrebuji pomoc."
                })
            if p.get("predchozi_job_id"):
                flags.append({
                    "typ": "opakovane",
                    "barva": "#7c3aed",
                    "bg": "#faf5ff",
                    "text": "Opakovane zadani",
                    "duvod": "Tato pozice uz byla vypsana drive (od {}) a zanikla. Firma ji zadala znovu pod novym inzeratem. Nesli obsadit na prvni pokus.".format(
                        p["predchozi_datum_prvni"][:10] if p.get("predchozi_datum_prvni") else "?"
                    )
                })
            if p.get("pocet_scanu", 0) >= 6:
                flags.append({
                    "typ": "dlouhodobe",
                    "barva": "#dc2626",
                    "bg": "#fef2f2",
                    "text": "{}x videno ve scanech".format(p["pocet_scanu"]),
                    "duvod": "Pozici jsme videli v {} ruznych scanech. Je otevrena uz minimalne {} dni. Firma ji dlouhodobe nedokaze obsadit.".format(
                        p["pocet_scanu"], p.get("dni_aktivni", "?")
                    )
                })
            elif p.get("pocet_scanu", 0) >= 4:
                flags.append({
                    "typ": "problematicke",
                    "barva": "#ea580c",
                    "bg": "#fff7ed",
                    "text": "{}x videno ve scanech".format(p["pocet_scanu"]),
                    "duvod": "Pozice je otevrena uz dele nez mesic (videna v {} scanech). Firma ji pravdepodobne neobsadi sama.".format(
                        p["pocet_scanu"]
                    )
                })
            if p.get("dni_aktivni") and p["dni_aktivni"] > 30 and not any(f["typ"] in ("dlouhodobe","problematicke") for f in flags):
                flags.append({
                    "typ": "starnouci",
                    "barva": "#d97706",
                    "bg": "#fefce8",
                    "text": "{}d otevrena".format(p["dni_aktivni"]),
                    "duvod": "Pozice je otevrena {} dni. Cim dele, tim vetsi pravdepodobnost ze firma potrebuje externi pomoc s naborem.".format(
                        p["dni_aktivni"]
                    )
                })
            p["flags"] = flags

        # Decision makers
        dm_norm = firma_norm
        dms = [dict(r) for r in conn.execute("""
            SELECT * FROM decision_makers
            WHERE company_normalized = ?
        """, (dm_norm,)).fetchall()]

        # Also try substring match for DMs
        if not dms:
            all_dm_companies = conn.execute(
                "SELECT DISTINCT company_normalized FROM decision_makers WHERE company_normalized != ''"
            ).fetchall()
            for row in all_dm_companies:
                cn = row["company_normalized"]
                if len(cn) >= 4 and len(firma_norm) >= 4 and (cn in firma_norm or firma_norm in cn):
                    dm_norm = cn
                    dms = [dict(r) for r in conn.execute(
                        "SELECT * FROM decision_makers WHERE company_normalized = ?",
                        (dm_norm,)
                    ).fetchall()]
                    break

        # Outreach history
        outreach_history = [dict(r) for r in conn.execute("""
            SELECT * FROM outreach WHERE firma_norm = ?
            ORDER BY datum_osloveni DESC
        """, (firma_norm,)).fetchall()]

        # Compute stats
        aktivni_pozice = [p for p in pozice if p["aktivni"]]
        neaktivni_pozice = [p for p in pozice if not p["aktivni"]]
        kraje = sorted(set(p["kraj"] for p in aktivni_pozice if p.get("kraj")))

        # Aggregate flags for active positions
        cnt_aktualizovano = sum(1 for p in aktivni_pozice if any(f["typ"] == "aktualizovano" for f in p["flags"]))
        cnt_opakovane = sum(1 for p in aktivni_pozice if any(f["typ"] == "opakovane" for f in p["flags"]))
        cnt_problematicke = sum(1 for p in aktivni_pozice if p.get("pocet_scanu", 0) >= 4)

        # Signal summary
        signals = []
        if cnt_aktualizovano:
            signals.append("{} aktualizovanych".format(cnt_aktualizovano))
        if cnt_opakovane:
            signals.append("{} opakovane zadanych".format(cnt_opakovane))
        if cnt_problematicke:
            signals.append("{} dlouhodobe neobsazenych".format(cnt_problematicke))

        # Days since oldest active position
        max_days = 0
        for p in aktivni_pozice:
            if p.get("dni_aktivni") and p["dni_aktivni"] > max_days:
                max_days = p["dni_aktivni"]

        return {
            "firma": target_firma,
            "firma_norm": firma_norm,
            "pozice_aktivni": aktivni_pozice,
            "pozice_neaktivni": neaktivni_pozice,
            "decision_makers": dms,
            "outreach": outreach_history,
            "kraje": kraje,
            "pocet_aktivni": len(aktivni_pozice),
            "pocet_neaktivni": len(neaktivni_pozice),
            "max_days": max_days,
            "cnt_aktualizovano": cnt_aktualizovano,
            "cnt_opakovane": cnt_opakovane,
            "cnt_problematicke": cnt_problematicke,
            "signals": signals,
        }


# ── Dashboard stats ──────────────────────────────────────────────────────────

def dashboard_stats() -> dict:
    """Agregované statistiky pro dashboard."""
    week_ago = (date.today() - timedelta(days=7)).isoformat()
    na = "AND is_agency = 0"

    with get_conn() as conn:
        aktivni = conn.execute(f"SELECT COUNT(*) FROM nabidky WHERE aktivni = 1 {na}").fetchone()[0]
        total_db = conn.execute("SELECT COUNT(*) FROM nabidky WHERE is_agency = 0").fetchone()[0]
        nove_tyden = conn.execute(
            f"SELECT COUNT(*) FROM nabidky WHERE DATE(datum_prvni_scan) >= ? {na}", (week_ago,)
        ).fetchone()[0]
        aktualizovane = conn.execute(
            f"SELECT COUNT(*) FROM nabidky WHERE aktivni = 1 AND publikovano LIKE '%ktualizov%' {na}"
        ).fetchone()[0]
        starnouci = conn.execute(
            f"SELECT COUNT(*) FROM nabidky WHERE aktivni = 1 {na} "
            "AND CAST(JULIANDAY('now') - JULIANDAY(datum_prvni_scan) AS INTEGER) >= 21"
        ).fetchone()[0]
        opakovane = conn.execute(
            f"SELECT COUNT(*) FROM nabidky WHERE aktivni = 1 {na} "
            "AND predchozi_job_id IS NOT NULL AND predchozi_job_id != ''"
        ).fetchone()[0]
        problematicke = conn.execute(
            f"SELECT COUNT(*) FROM nabidky WHERE aktivni = 1 AND pocet_scanu >= 4 {na}"
        ).fetchone()[0]
        firmy_celkem = conn.execute(
            f"SELECT COUNT(DISTINCT firma) FROM nabidky WHERE aktivni = 1 AND firma != '' {na}"
        ).fetchone()[0]
        dm_total = conn.execute("SELECT COUNT(*) FROM decision_makers").fetchone()[0]
        outreach_total = conn.execute("SELECT COUNT(*) FROM outreach").fetchone()[0]
        outreach_ceka = conn.execute(
            "SELECT COUNT(*) FROM outreach WHERE status = 'odeslano'"
        ).fetchone()[0]
        outreach_odpoved = conn.execute(
            "SELECT COUNT(*) FROM outreach WHERE status IN ('odpovedela','schuzka','klient')"
        ).fetchone()[0]
        posledni_scan = conn.execute(
            "SELECT hodnota FROM konfigurace WHERE klic = 'posledni_scan'"
        ).fetchone()
        # Recent outreach activity
        recent_outreach = [dict(r) for r in conn.execute(
            "SELECT * FROM outreach ORDER BY datum_aktualizace DESC LIMIT 5"
        ).fetchall()]

    return {
        "aktivni_pozice": aktivni,
        "total_pozice": total_db,
        "nove_tyden": nove_tyden,
        "aktualizovane": aktualizovane,
        "starnouci": starnouci,
        "opakovane": opakovane,
        "problematicke": problematicke,
        "firmy_celkem": firmy_celkem,
        "dm_total": dm_total,
        "outreach_total": outreach_total,
        "outreach_ceka": outreach_ceka,
        "outreach_odpoved": outreach_odpoved,
        "posledni_scan": posledni_scan[0] if posledni_scan else "—",
        "recent_outreach": recent_outreach,
    }


def filtr_pozice(filtr: str = "", kraj: str = "", limit: int = 500,
                 sort: str = "") -> list:
    """Vrací filtrované pozice s kontextem firmy.
    filtr: 'aktualizovane' | 'starnouci' | 'nove_tyden' | 'aktivni' |
           'opakovane' | 'problematicke' | 'nejstarsi' | ''
    sort: 'stari' | 'firma' | 'scanu' | '' (default per filter)
    Each row is enriched with company stats: firma_pozic, firma_opak, firma_probl, firma_aktual.
    """
    week_ago = (date.today() - timedelta(days=7)).isoformat()

    base = """
        SELECT n.firma, n.pozice, n.kraj, n.misto, n.url, n.plat_text,
               n.publikovano, n.datum_vydani, n.datum_prvni_scan,
               n.pocet_scanu, n.predchozi_job_id, n.predchozi_datum_prvni,
               CAST(JULIANDAY('now') - JULIANDAY(n.datum_prvni_scan) AS INTEGER) AS dni_aktivni
        FROM nabidky n
        WHERE n.aktivni = 1 AND n.is_agency = 0
    """
    params = []

    if filtr == "aktualizovane":
        base += " AND n.publikovano LIKE '%ktualizov%'"
    elif filtr == "starnouci":
        base += " AND CAST(JULIANDAY('now') - JULIANDAY(n.datum_prvni_scan) AS INTEGER) >= 21"
    elif filtr == "nove_tyden":
        base += " AND DATE(n.datum_prvni_scan) >= ?"
        params.append(week_ago)
    elif filtr == "opakovane":
        base += " AND n.predchozi_job_id IS NOT NULL AND n.predchozi_job_id != ''"
    elif filtr == "problematicke":
        base += " AND n.pocet_scanu >= 4"
    elif filtr == "nejstarsi":
        pass  # all active, sorted by age

    if kraj:
        base += " AND n.kraj = ?"
        params.append(kraj)

    # Determine sort order
    if sort == "stari":
        base += " ORDER BY dni_aktivni DESC"
    elif sort == "firma":
        base += " ORDER BY n.firma ASC, dni_aktivni DESC"
    elif sort == "scanu":
        base += " ORDER BY n.pocet_scanu DESC"
    elif filtr in ("aktualizovane", "starnouci", "problematicke", "nejstarsi"):
        base += " ORDER BY dni_aktivni DESC"
    elif filtr in ("opakovane",):
        base += " ORDER BY n.datum_prvni_scan DESC"
    elif filtr == "nove_tyden":
        base += " ORDER BY n.datum_prvni_scan DESC"
    else:
        base += " ORDER BY n.datum_prvni_scan DESC"

    base += " LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(base, params).fetchall()
        positions = [dict(r) for r in rows]

        # Enrich: company stats (batch query for all firms in the result)
        if positions:
            firms = list({p["firma"] for p in positions if p["firma"]})
            if firms:
                placeholders = ",".join("?" * len(firms))
                firma_stats = conn.execute(f"""
                    SELECT firma,
                           COUNT(*) as firma_pozic,
                           SUM(CASE WHEN predchozi_job_id IS NOT NULL
                                    AND predchozi_job_id != '' THEN 1 ELSE 0 END) as firma_opak,
                           SUM(CASE WHEN pocet_scanu >= 4 THEN 1 ELSE 0 END) as firma_probl,
                           SUM(CASE WHEN publikovano LIKE '%ktualizov%' THEN 1 ELSE 0 END) as firma_aktual
                    FROM nabidky
                    WHERE aktivni = 1 AND is_agency = 0 AND firma IN ({placeholders})
                    GROUP BY firma
                """, firms).fetchall()
                stats_map = {r["firma"]: dict(r) for r in firma_stats}

                for p in positions:
                    fs = stats_map.get(p["firma"], {})
                    p["firma_pozic"] = fs.get("firma_pozic", 0)
                    p["firma_opak"] = fs.get("firma_opak", 0)
                    p["firma_probl"] = fs.get("firma_probl", 0)
                    p["firma_aktual"] = fs.get("firma_aktual", 0)

    return positions


# ── Company overview (browsable list) ─────────────────────────────────────────

def firmy_prehled(sort: str = "pozic", page: int = 1, per_page: int = 50,
                  kraj: str = "") -> dict:
    """Returns paginated company list with sorting.
    sort: 'pozic' | 'opakovane' | 'problematicke' | 'firma'
    Returns: {firmy: [...], total: int, page: int, pages: int}
    """
    na = "AND is_agency = 0"
    kraj_filter = ""
    params = []
    if kraj:
        kraj_filter = "AND kraj = ?"
        params.append(kraj)

    order = {
        "pozic": "pocet_pozic DESC",
        "opakovane": "opakovanych DESC, pocet_pozic DESC",
        "problematicke": "problematickych DESC, pocet_pozic DESC",
        "firma": "firma ASC",
    }.get(sort, "pocet_pozic DESC")

    with get_conn() as conn:
        # Total count
        total = conn.execute(f"""
            SELECT COUNT(DISTINCT firma) FROM nabidky
            WHERE aktivni = 1 AND firma != '' {na} {kraj_filter}
        """, params).fetchone()[0]

        offset = (page - 1) * per_page
        pages = max(1, (total + per_page - 1) // per_page)

        rows = conn.execute(f"""
            SELECT firma,
                   COUNT(*) as pocet_pozic,
                   SUM(CASE WHEN predchozi_job_id IS NOT NULL
                            AND predchozi_job_id != '' THEN 1 ELSE 0 END) as opakovanych,
                   SUM(CASE WHEN pocet_scanu >= 4 THEN 1 ELSE 0 END) as problematickych,
                   MAX(pocet_scanu) as max_scanu,
                   MIN(datum_prvni_scan) as nejstarsi,
                   GROUP_CONCAT(DISTINCT kraj) as kraje
            FROM nabidky
            WHERE aktivni = 1 AND firma != '' {na} {kraj_filter}
            GROUP BY firma
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """, params + [per_page, offset]).fetchall()

    firmy = []
    for r in rows:
        firmy.append({
            "firma": r["firma"],
            "firma_norm": normalize_company(r["firma"]),
            "pocet_pozic": r["pocet_pozic"],
            "opakovanych": r["opakovanych"],
            "problematickych": r["problematickych"],
            "max_scanu": r["max_scanu"],
            "nejstarsi": r["nejstarsi"],
            "kraje": r["kraje"] or "",
        })

    return {"firmy": firmy, "total": total, "page": page, "pages": pages}


# ── Outreach CRM ─────────────────────────────────────────────────────────────

OUTREACH_STATUSES = [
    "odeslano",       # Message sent
    "zobrazeno",      # Seen / opened
    "odpovedela",     # Replied
    "schuzka",        # Meeting scheduled
    "klient",         # Became client
    "odmitnuto",      # Declined
    "follow_up",      # Needs follow-up
]


def add_outreach(firma: str, firma_norm: str, kontakt_jmeno: str,
                 kontakt_url: str = "", kanal: str = "LinkedIn DM",
                 poznamka: str = "", kraj: str = "") -> int:
    """Zaznamená oslovení firmy. Vrací ID záznamu."""
    now = datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        cursor = conn.execute("""
            INSERT INTO outreach (firma, firma_norm, kontakt_jmeno, kontakt_url,
                                  kanal, status, poznamka, datum_osloveni,
                                  datum_aktualizace, kraj)
            VALUES (?, ?, ?, ?, ?, 'odeslano', ?, ?, ?, ?)
        """, (firma, firma_norm, kontakt_jmeno, kontakt_url,
              kanal, poznamka, now, now, kraj))
        return cursor.lastrowid


def update_outreach_status(outreach_id: int, status: str, poznamka: str = "") -> None:
    """Aktualizuje status oslovení."""
    now = datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        if poznamka:
            conn.execute("""
                UPDATE outreach SET status = ?, poznamka = ?, datum_aktualizace = ?
                WHERE id = ?
            """, (status, poznamka, now, outreach_id))
        else:
            conn.execute("""
                UPDATE outreach SET status = ?, datum_aktualizace = ?
                WHERE id = ?
            """, (status, now, outreach_id))


def nacti_outreach(status: str = "", limit: int = 100) -> list:
    """Načte záznamy oslovení."""
    with get_conn() as conn:
        where, params = [], []
        if status:
            where.append("status = ?")
            params.append(status)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        params += [limit]
        return [dict(r) for r in conn.execute(
            f"""SELECT * FROM outreach {clause}
                ORDER BY datum_osloveni DESC LIMIT ?""",
            params
        ).fetchall()]


def outreach_statistiky() -> dict:
    """Statistiky oslovení."""
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM outreach").fetchone()[0]
        per_status = conn.execute("""
            SELECT status, COUNT(*) as pocet FROM outreach
            GROUP BY status ORDER BY pocet DESC
        """).fetchall()
        recent = conn.execute("""
            SELECT * FROM outreach ORDER BY datum_osloveni DESC LIMIT 20
        """).fetchall()
    return {
        "total": total,
        "per_status": [dict(r) for r in per_status],
        "recent": [dict(r) for r in recent],
    }


# ── Per-region recommendations ───────────────────────────────────────────────

def radar_doporuceni(per_region: int = 3, skip_fuzzy: bool = True) -> list:
    """Top N firem za každý region, bez již oslovených.
    Vrací flat list seřazený: region → signal_score DESC.
    skip_fuzzy=True by default for speed (exact+substring matches are sufficient).
    """
    result = radar_matches(skip_fuzzy=skip_fuzzy)
    matches = result["matches"]

    # Group by region, exclude already contacted
    by_region = {}
    for m in matches:
        if m["outreach_status"]:
            continue  # Already contacted, skip
        for kraj in m["kraje"]:
            if kraj not in by_region:
                by_region[kraj] = []
            by_region[kraj].append(m)

    # Take top N per region
    recommendations = []
    seen_firms = set()
    for kraj in sorted(by_region.keys()):
        count = 0
        for m in by_region[kraj]:
            if m["firma_norm"] in seen_firms:
                continue
            recommendations.append({**m, "doporuceny_kraj": kraj})
            seen_firms.add(m["firma_norm"])
            count += 1
            if count >= per_region:
                break

    return recommendations


# ── Alerts: sudden position increase ─────────────────────────────────────────

def detect_surge(threshold: int = 4) -> list:
    """Detekuje firmy, které tento týden přidaly 4+ nových pozic.
    Porovnává datum_prvni_scan z posledních 7 dní vs. starší.
    """
    week_ago = (date.today() - timedelta(days=7)).isoformat()

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT firma, kraj,
                   COUNT(*) as nove_pozice,
                   GROUP_CONCAT(pozice, ' | ') as pozice_list
            FROM nabidky
            WHERE aktivni = 1
              AND DATE(datum_prvni_scan) >= ?
              AND firma != ''
              AND is_agency = 0
            GROUP BY firma
            HAVING COUNT(*) >= ?
            ORDER BY COUNT(*) DESC
            LIMIT 20
        """, (week_ago, threshold)).fetchall()

    surges = []
    for r in rows:
        surges.append({
            "firma": r["firma"],
            "firma_norm": normalize_company(r["firma"]),
            "kraj": r["kraj"],
            "nove_pozice": r["nove_pozice"],
            "pozice_list": r["pozice_list"].split(" | ")[:5],
        })
    return surges


# ── Batch outreach message generator ─────────────────────────────────────────

DEFAULT_TEMPLATE = """Dobrý den, {osloveni},

všiml/a jsem si, že ve firmě {firma} aktuálně hledáte nové kolegy{pozice_text}. Vím, jak náročné může být obsadit správné pozice v dnešním trhu.

Jsme Sintera — pomáháme firmám v regionu s náborem klíčových lidí. Rádi bychom Vám nezávazně představili, jak bychom mohli pomoct.

Bylo by možné si krátce napsat nebo zavolat?

S pozdravem,
Pavel Kubížňák
Sintera"""


def generate_batch_messages(recommendations: list, template: str = "") -> list:
    """Generuje personalizované zprávy pro doporučené firmy."""
    if not template:
        template = DEFAULT_TEMPLATE

    messages = []
    for m in recommendations:
        dm = m["decision_makers"][0] if m["decision_makers"] else None
        if not dm:
            continue

        # Build osloveni (paní/pane + příjmení)
        last_name = dm.get("last_name", "")
        full_name = dm.get("full_name", "")
        # Simple Czech gender detection: female last names typically end in -ová, -á
        if last_name and (last_name.endswith("ová") or last_name.endswith("á")
                          or last_name.endswith("ová,")):
            osloveni = "paní {}".format(last_name)
        elif last_name:
            osloveni = "pane {}".format(last_name)
        else:
            osloveni = full_name

        # Build pozice text
        pozice_names = [p["pozice"] for p in m["pozice"][:3]]
        if len(pozice_names) == 1:
            pozice_text = " — konkrétně na pozici {}".format(pozice_names[0])
        elif pozice_names:
            pozice_text = " — například {}".format(", ".join(pozice_names[:2]))
        else:
            pozice_text = ""

        msg = template.format(
            osloveni=osloveni,
            firma=m["firma"],
            pozice_text=pozice_text,
            pocet_pozic=m["pocet_pozic"],
            kraj=m.get("doporuceny_kraj", ", ".join(m["kraje"])),
        )

        messages.append({
            "firma": m["firma"],
            "firma_norm": m["firma_norm"],
            "kontakt": dm["full_name"],
            "kontakt_title": dm["current_title"],
            "linkedin_url": dm["profile_url"],
            "kraj": m.get("doporuceny_kraj", ", ".join(m["kraje"])),
            "signal": m["signal"],
            "pocet_pozic": m["pocet_pozic"],
            "zprava": msg,
        })

    return messages


# ── MISSILE: Cold LinkedIn DM Generator ──────────────────────────────────────
#
# Framework: Braun / Reamer / Holland / Allred / Mowinski synthesis
#
# Struktura zprávy:
#   1. Fakt — konkrétní, měřitelný, nepopiratelný. Dokazuje, že nepíšeme naslepo.
#   2. Interpretace fáze trhu — NE reportáž ("vidím, že hledáte"), ALE diagnóza
#      ("v téhle fázi bývá aktivní trh vyčerpaný"). Říkáme, co to ZNAMENÁ.
#   3. Malý next step — snížit tření odpovědi na minimum.
#
# Dvě CTA polarity:
#   DIRECT (Reamer/Berman): "Pošlete číslo, řeknu během 2 minut, jestli bych
#     to ještě čekal, nebo měnil postup."
#   SOFT (Braun/Allred): "Mám k tomu jednu stručnou poznámku. Chcete ji sem,
#     nebo je rychlejší telefon?"
#
# Pravidla:
#   - Žádný "Dobrý den", žádné představování, žádný název firmy
#   - Podpis jen "Šárka"
#   - Zakázaná slova: nabídka, klient, recruitment, agentura, headhunter,
#     outsourcing, pomoc, analýza, audit
#   - Nepsat o roli. Psát o fázi trhu.
#   - Max 300 znaků
#   - Cíl: příjemce si řekne "jak to ví?" — a odpoví.

_MISSILE_PRIORITY = {"A2": 6, "A3": 5, "A1": 4, "D": 3, "E": 2, "F": 1}


def _gender_osloveni(dm: dict) -> str:
    """'paní Nováková' / 'pane Novák'. Handles accented + unaccented Czech."""
    last = dm.get("last_name", "").strip()
    first = dm.get("first_name", "").strip()
    if not last or len(last) <= 1:
        full = dm.get("full_name", "").strip()
        parts = full.split()
        if len(parts) >= 2:
            last = parts[-1]
        elif full:
            return full
        else:
            return ""
    # Strip trailing titles: ", MBA", ", DiS.", " PhD" etc.
    clean = re.sub(r'[,\s]+(MBA|PhD|CSc|Ing|Mgr|Bc|DiS|Dr|Ph\.?D\.?)\s*\.?\s*$',
                   '', last, flags=re.IGNORECASE).strip()
    clean = clean.rstrip(',').strip()
    if not clean or len(clean) <= 1:
        return dm.get("full_name", "").strip()
    # Normalize case: if all-lower or all-upper, title-case it
    if clean.islower() or clean.isupper():
        clean = clean.title()
    low = clean.lower()
    # Czech female surname patterns (accented + unaccented):
    #   -ová/-ova (Nováková, Bukovova)
    #   -ská/-ska (Bukovská/Bukovska, Černohorská/Cernohorska)
    #   -cká/-cka (Kopecká)
    #   -á (Krátká, Dlouhá) — only accented, bare -a is too ambiguous
    if (low.endswith("ová") or low.endswith("ova")
            or low.endswith("ská") or low.endswith("ska")
            or low.endswith("cká") or low.endswith("cka")
            or (low.endswith("á") and len(clean) > 2)):
        return "paní {}".format(clean)
    # Fallback: Czech female first names almost always end in -a or -e/-ie
    # Male names end in consonant, -o, -ek, -ík, -oslav, etc.
    if first:
        fl = first.lower().rstrip('.')
        if fl.endswith("a") or fl.endswith("ie") or fl.endswith("na"):
            return "paní {}".format(clean)
    return "pane {}".format(clean)


def _compute_dni(p: dict) -> int:
    """Days active for a position."""
    dni = p.get("dni_aktivni")
    if dni is not None:
        return int(dni)
    try:
        d1 = datetime.fromisoformat(p["datum_prvni_scan"][:10])
        return (datetime.now() - d1).days
    except Exception:
        return 0


def _missile_archetype(firma_data: dict) -> tuple:
    """Best MISSILE archetype for a company.
    Returns (code, reference_position, extra_data).
    extra_data always includes computed metrics for the message generator.
    """
    pozice = firma_data.get("pozice", [])
    if not pozice:
        return ("F", None, {})

    # Pre-compute metrics for all positions
    total_pozic = len(pozice)
    max_scanu = max(p.get("pocet_scanu", 1) for p in pozice)

    # A2 — Republished (aktualizovano)
    for p in pozice:
        pub = p.get("publikovano") or ""
        if "ktualizov" in pub:
            dni = _compute_dni(p)
            return ("A2", p, {
                "dni": dni, "scanu": p.get("pocet_scanu", 1),
                "total_pozic": total_pozic,
            })

    # A3 — Repeated posting
    for p in pozice:
        if p.get("predchozi_job_id"):
            predchozi_raw = (p.get("predchozi_datum_prvni") or "")[:10]
            datum_display = ""
            if predchozi_raw and len(predchozi_raw) >= 10:
                try:
                    pd = datetime.fromisoformat(predchozi_raw)
                    now = datetime.now()
                    _cz_gen = {1:"ledna",2:"února",3:"března",4:"dubna",5:"května",
                               6:"června",7:"července",8:"srpna",9:"září",
                               10:"října",11:"listopadu",12:"prosince"}
                    datum_display = "{}. {}".format(pd.day, _cz_gen.get(pd.month, str(pd.month)))
                except Exception:
                    datum_display = predchozi_raw
            return ("A3", p, {
                "datum": datum_display,
                "scanu": p.get("pocet_scanu", 1),
                "total_pozic": total_pozic,
            })

    # A1 — Long-running: pick position with highest scan count
    best_a1 = None
    best_scanu = 0
    for p in pozice:
        scanu = p.get("pocet_scanu", 1)
        dni = _compute_dni(p)
        if scanu >= 4 or dni >= 30:
            if scanu > best_scanu or (scanu == best_scanu and dni > _compute_dni(best_a1 or {})):
                best_a1 = p
                best_scanu = scanu
    if best_a1:
        dni = _compute_dni(best_a1)
        return ("A1", best_a1, {
            "dni": dni, "scanu": best_scanu,
            "total_pozic": total_pozic,
        })

    # D — Hiring wave (3+ positions)
    if total_pozic >= 3:
        names = [p["pozice"] for p in pozice[:3]]
        return ("D", pozice[0], {"pocet": total_pozic, "names": names})

    # E — Missing salary
    for p in pozice:
        if not p.get("plat_text"):
            return ("E", p, {"total_pozic": total_pozic})

    # F — Fallback
    return ("F", pozice[0], {"total_pozic": total_pozic})


def _missile_text(archetype: str, osloveni: str, firma_data: dict,
                  ref_pozice: dict, kraj: str, extra: dict) -> dict:
    """v3 — Braun/Reamer/Holland/Allred/Mowinski synthesis.

    Structure:  Fact → Market-phase interpretation → CTA
    Returns {'direct': str, 'soft': str} — two CTA polarities.

    DIRECT (Reamer/Berman): "Pošlete číslo, řeknu rovnou..."
    SOFT   (Braun/Allred):  "Mám k tomu jednu poznámku. Chcete ji sem...?"

    Rules:
    - No greeting, no introduction, no company name. Sign "Šárka".
    - Write about market PHASE, not the role itself.
    - Forbidden: nabídka, klient, recruitment, agentura, headhunter,
      outsourcing, pomoc, analýza, audit
    - Max 300 chars per variant.
    """
    poz = ref_pozice["pozice"] if ref_pozice else ""
    scanu = extra.get("scanu", (ref_pozice or {}).get("pocet_scanu", 1))
    dni = extra.get("dni", _compute_dni(ref_pozice) if ref_pozice else 0)

    # Deterministic sub-variant per company (consistent across reloads)
    v = sum(ord(c) * (i + 1) for i, c in enumerate(
        firma_data.get("firma_norm", "x"))) % 3

    # ── A2: Republished ad ──────────────────────────────────────────
    if archetype == "A2":
        if v == 0:
            fi = (f"{osloveni}, {poz} — obnoveno po {scanu} kolech. "
                  f"V téhle fázi stejný inzerát osloví stejné lidi "
                  f"— kdo měl reagovat, už reagoval.")
        elif v == 1:
            fi = (f"{osloveni}, {poz} — {scanu}x ve scanu, teď obnoveno. "
                  f"Obnovení nepřitáhne jiné lidi "
                  f"— aktivní trh na tuhle roli je vyčerpaný.")
        else:
            fi = (f"{osloveni}, {poz} jste obnovili. "
                  f"V téhle fázi problém nebývá v inzerátu, "
                  f"ale v tom, že kanál je vyčerpaný.")
        dc = ("Pošlete číslo, řeknu rovnou, jestli bych to ještě zkoušel "
              "takhle, nebo měnil postup. — Šárka")
        sc = ("Mám k tomu jednu stručnou poznámku. "
              "Chcete ji sem, nebo je rychlejší telefon? — Šárka")

    # ── A3: Repeated posting ────────────────────────────────────────
    elif archetype == "A3":
        datum = extra.get("datum", "")
        if datum:
            if v == 0:
                fi = (f"{osloveni}, {poz} — podruhé od {datum}. "
                      f"Druhé kolo přes portál většinou přivede "
                      f"ty samé CV jako první.")
            elif v == 1:
                fi = (f"{osloveni}, {poz} — znovu, "
                      f"předchozí od {datum} zanikla. "
                      f"V téhle fázi portál obvykle recykluje "
                      f"stejné kandidáty.")
            else:
                fi = (f"{osloveni}, {poz} — druhé zadání od {datum}. "
                      f"To většinou znamená, že aktivní trh "
                      f"tuhle roli nevyřešil napoprvé.")
        else:
            fi = (f"{osloveni}, {poz} — vypsáno podruhé. "
                  f"V téhle fázi portál přivádí stejné lidi "
                  f"jako poprvé.")
        dc = ("Pošlete číslo, řeknu za minutu, "
              "jestli to ještě řešit standardně. — Šárka")
        sc = ("Mám k tomu stručný postřeh z oboru. "
              "Chcete ho sem, nebo je rychlejší telefon? — Šárka")

    # ── A1: Long-running (main archetype) ───────────────────────────
    elif archetype == "A1":
        if scanu >= 6:
            if v == 0:
                fi = (f"{osloveni}, {poz} — {scanu}x ve scanu. "
                      f"V téhle fázi bývá aktivní trh "
                      f"obvykle vyčerpaný.")
            elif v == 1:
                fi = (f"{osloveni}, {poz} — {scanu}x ve scanu "
                      f"a v {kraj} běží podobných rolí víc. "
                      f"To většinou znamená, že lidi, "
                      f"co aktivně hledají, už jsou pryč.")
            else:
                fi = (f"{osloveni}, {poz} visí {scanu} kol ve scanu. "
                      f"V téhle fázi problém většinou nebývá "
                      f"v inzerátu, ale v tom, že aktivní trh "
                      f"tu roli nevyřeší.")
        else:
            # 4–5 scans
            if v == 0:
                fi = (f"{osloveni}, {poz} — {scanu}x ve scanu. "
                      f"V téhle fázi portál obvykle přestává "
                      f"přivádět nové lidi.")
            elif v == 1:
                fi = (f"{osloveni}, {poz} — {scanu}. kolo ve scanu. "
                      f"To většinou znamená, že kdo měl "
                      f"z portálu reagovat, už reagoval.")
            else:
                fi = (f"{osloveni}, {poz} — {scanu}x ve scanu "
                      f"v {kraj}. V téhle fázi aktivní trh bývá "
                      f"z velké části vyčerpaný.")
        dc = ("Pošlete číslo, řeknu rovnou, jestli bych to ještě "
              "čekal, nebo měnil postup. — Šárka")
        sc = ("Mám k tomu jednu stručnou poznámku. "
              "Chcete ji sem, nebo je rychlejší telefon? — Šárka")

    # ── D: Hiring wave ──────────────────────────────────────────────
    elif archetype == "D":
        pocet = extra.get("pocet", 3)
        names = extra.get("names", [])
        if pocet > 5:
            fi = (f"{osloveni}, {pocet} otevřených pozic najednou. "
                  f"Při tomhle objemu aktivní trh nestačí "
                  f"— nejlepší zmizí do dvou dnů.")
        else:
            names_str = ", ".join(names[:2])
            fi = (f"{osloveni}, {pocet} pozic paralelně — {names_str}. "
                  f"V téhle fázi portál sám nestíhá "
                  f"pokrýt paralelní nábor.")
        dc = ("Pošlete číslo, řeknu rovnou, "
              "jestli to jde pokrýt rychleji. — Šárka")
        sc = ("Mám k tomu postřeh z regionu. "
              "Má smysl si to ověřit po telefonu? — Šárka")

    # ── E: Missing salary ───────────────────────────────────────────
    elif archetype == "E":
        if v % 2 == 0:
            fi = (f"{osloveni}, {poz} — bez platu v inzerátu. "
                  f"Odezva bývá o polovinu nižší "
                  f"— dobří kandidáti tyhle přeskakují.")
        else:
            fi = (f"{osloveni}, {poz} běží bez uvedeného platu. "
                  f"V téhle fázi dobří kandidáti většinou "
                  f"přeskočí na inzerát s čísly.")
        dc = ("Pošlete číslo, řeknu, jestli jsou "
              "na trhu a za kolik. — Šárka")
        sc = ("Mám k tomu jednu poznámku z trhu. "
              "Chcete ji sem? — Šárka")

    # ── F: Fallback ─────────────────────────────────────────────────
    else:
        pocet = extra.get("total_pozic", 1)
        if pocet > 1:
            fi = (f"{osloveni}, {pocet} otevřených pozic v {kraj}. "
                  f"Lidi, co to umí, na portálech aktivně nehledají.")
        else:
            fi = (f"{osloveni}, {poz} v {kraj}. "
                  f"Aktivní trh na tyhle role bývá omezený.")
        dc = "Pošlete číslo. — Šárka"
        sc = "Mám k tomu jednu poznámku. Chcete ji sem? — Šárka"

    return {"direct": f"{fi} {dc}", "soft": f"{fi} {sc}"}


_ARCHETYPE_LABELS = {
    "A2": "Obnovený inzerát",
    "A3": "Opakovaně zadáno",
    "A1": "Dlouho otevřená",
    "D": "Budují tým",
    "E": "Bez platu",
    "F": "Aktivní nábor",
}

_ARCHETYPE_COLORS = {
    "A2": ("#dc2626", "#fef2f2"),
    "A3": ("#7c3aed", "#faf5ff"),
    "A1": ("#ea580c", "#fff7ed"),
    "D": ("#2563eb", "#dbeafe"),
    "E": ("#d97706", "#fefce8"),
    "F": ("#64748b", "#f8fafc"),
}


def generate_missile_dms(kraj: str = "", limit: int = 50) -> list:
    """MISSILE cold DM generator.

    Fact → diagnosis → "Pošlete číslo."
    Three sentences. No greetings. No introductions. No company name.

    Returns list sorted by archetype priority + signal score.
    Excludes already-contacted companies.
    """
    result = radar_matches(skip_fuzzy=True)
    matches = result.get("matches", [])

    dms = []
    for m in matches:
        if m.get("outreach_status"):
            continue
        if not m.get("decision_makers"):
            continue
        if kraj and kraj not in m.get("kraje", []):
            continue

        archetype, ref_pozice, extra = _missile_archetype(m)

        dm = m["decision_makers"][0]
        # Skip bad DM data: short names, incomplete records
        last = dm.get("last_name", "").strip()
        full = dm.get("full_name", "").strip()
        if len(full) < 5 or len(last) <= 2:
            continue
        # Skip if last name looks like garbage (no uppercase letter)
        clean_last = re.sub(r'[,\s]+(MBA|PhD|CSc|Ing|Mgr|Bc|DiS).*$', '', last, flags=re.IGNORECASE).strip().rstrip(',')
        if clean_last and not any(c.isupper() for c in clean_last):
            continue
        osloveni = _gender_osloveni(dm)
        if not osloveni or len(osloveni) < 5:
            continue

        pozice_name = ref_pozice["pozice"] if ref_pozice else m["pozice"][0]["pozice"]
        msg_kraj = kraj or (m["kraje"][0] if m["kraje"] else "ČR")

        texts = _missile_text(archetype, osloveni, m, ref_pozice, msg_kraj, extra)

        color, bg = _ARCHETYPE_COLORS.get(archetype, ("#64748b", "#f8fafc"))
        priority = _MISSILE_PRIORITY.get(archetype, 0)

        dms.append({
            "firma": m["firma"],
            "firma_norm": m["firma_norm"],
            "kontakt": dm["full_name"],
            "kontakt_title": dm.get("current_title", ""),
            "linkedin_url": dm.get("profile_url", ""),
            "kraj": msg_kraj,
            "signal": m.get("signal", ""),
            "signal_score": m.get("signal_score", 0),
            "pocet_pozic": m["pocet_pozic"],
            "archetype": archetype,
            "archetype_label": _ARCHETYPE_LABELS.get(archetype, ""),
            "archetype_color": color,
            "archetype_bg": bg,
            "priority": priority,
            "pozice_ref": pozice_name,
            "zprava": texts["direct"],
            "zprava_soft": texts["soft"],
            "char_count": len(texts["direct"]),
            "char_count_soft": len(texts["soft"]),
        })

    dms.sort(key=lambda d: (d["priority"], d["signal_score"]), reverse=True)
    return dms[:limit]


# ── Salary benchmark ──────────────────────────────────────────────────────────

def salary_benchmark(kraj: str = "", keyword: str = "") -> dict:
    """Salary statistics by region and/or position keyword.

    Returns aggregated stats: avg, median, min, max, count
    grouped by region and position keyword.
    """
    na = "AND is_agency = 0"

    with get_conn() as conn:
        # Overall stats by region
        where_parts = ["aktivni = 1", "plat_od > 0", "is_agency = 0"]
        params = []

        if kraj:
            where_parts.append("kraj = ?")
            params.append(kraj)
        if keyword:
            where_parts.append("LOWER(pozice) LIKE ?")
            params.append("%{}%".format(keyword.lower()))

        where_clause = " AND ".join(where_parts)

        # Per-region breakdown
        region_stats = [dict(r) for r in conn.execute(f"""
            SELECT kraj,
                   COUNT(*) as pocet,
                   CAST(AVG(plat_od) AS INTEGER) as avg_od,
                   CAST(AVG(plat_do) AS INTEGER) as avg_do,
                   MIN(plat_od) as min_plat,
                   MAX(plat_do) as max_plat,
                   CAST(AVG((plat_od + plat_do) / 2) AS INTEGER) as avg_stred
            FROM nabidky
            WHERE {where_clause} AND kraj != ''
            GROUP BY kraj
            ORDER BY avg_stred DESC
        """, params).fetchall()]

        # Top positions by salary
        top_salary = [dict(r) for r in conn.execute(f"""
            SELECT pozice, firma, kraj, plat_text, plat_od, plat_do,
                   (plat_od + plat_do) / 2 as plat_stred
            FROM nabidky
            WHERE {where_clause}
            ORDER BY plat_stred DESC
            LIMIT 20
        """, params).fetchall()]

        # Position keyword breakdown (top groups)
        # Group by first significant word in pozice
        all_positions = conn.execute(f"""
            SELECT pozice, plat_od, plat_do, kraj
            FROM nabidky
            WHERE {where_clause}
        """, params).fetchall()

        # Aggregate by common position keywords
        keyword_stats = {}
        for row in all_positions:
            poz = row["pozice"].lower()
            # Extract key terms
            for term in _SALARY_KEYWORDS:
                if term in poz:
                    if term not in keyword_stats:
                        keyword_stats[term] = {"count": 0, "platy_od": [], "platy_do": []}
                    keyword_stats[term]["count"] += 1
                    keyword_stats[term]["platy_od"].append(row["plat_od"])
                    keyword_stats[term]["platy_do"].append(row["plat_do"])

        position_groups = []
        for term, data in sorted(keyword_stats.items(), key=lambda x: x[1]["count"], reverse=True):
            if data["count"] >= 2:
                od_list = sorted(data["platy_od"])
                do_list = sorted(data["platy_do"])
                mid = len(od_list) // 2
                position_groups.append({
                    "keyword": term,
                    "count": data["count"],
                    "avg_od": int(sum(od_list) / len(od_list)),
                    "avg_do": int(sum(do_list) / len(do_list)),
                    "median_od": od_list[mid],
                    "median_do": do_list[mid],
                    "min_plat": min(od_list),
                    "max_plat": max(do_list),
                })

        # Total stats
        total = conn.execute(f"""
            SELECT COUNT(*) as pocet,
                   CAST(AVG(plat_od) AS INTEGER) as avg_od,
                   CAST(AVG(plat_do) AS INTEGER) as avg_do,
                   MIN(plat_od) as min_plat,
                   MAX(plat_do) as max_plat,
                   CAST(AVG((plat_od + plat_do) / 2) AS INTEGER) as avg_stred
            FROM nabidky
            WHERE {where_clause}
        """, params).fetchone()

        # Count of positions without salary
        no_salary_params = []
        no_salary_where = ["aktivni = 1", "is_agency = 0", "(plat_od IS NULL OR plat_od = 0)"]
        if kraj:
            no_salary_where.append("kraj = ?")
            no_salary_params.append(kraj)
        if keyword:
            no_salary_where.append("LOWER(pozice) LIKE ?")
            no_salary_params.append("%{}%".format(keyword.lower()))

        bez_platu = conn.execute(
            "SELECT COUNT(*) FROM nabidky WHERE {}".format(" AND ".join(no_salary_where)),
            no_salary_params
        ).fetchone()[0]

    return {
        "region_stats": region_stats,
        "top_salary": top_salary,
        "position_groups": position_groups[:20],
        "total": dict(total) if total else {},
        "bez_platu": bez_platu,
        "filtr_kraj": kraj,
        "filtr_keyword": keyword,
    }


# Common position keywords for salary grouping
_SALARY_KEYWORDS = [
    "manager", "manažer", "ředitel", "director",
    "developer", "vývojář", "programátor", "engineer",
    "analyst", "analytik",
    "účetní", "accountant",
    "obchodní", "sales", "obchodník",
    "technik", "technician",
    "logistik", "logistics",
    "marketing", "marketér",
    "hr", "personalista",
    "quality", "kvalita",
    "project", "projektový",
    "it ", "it-",
    "java", "python", ".net", "c#", "react",
    "controller", "controlling",
    "nákupčí", "buyer", "procurement",
    "konstruktér", "designer",
    "elektro", "electrical",
    "strojní", "mechanical",
    "operátor", "operator",
    "vedoucí", "team lead",
    "data", "admin",
]


# ── Analytics export ──────────────────────────────────────────────────────────

def analytics_export_data() -> list:
    """Flat table of all active positions with computed boolean columns.
    Designed for Google Sheets → Looker Studio pipeline.
    """
    na = "AND is_agency = 0"

    with get_conn() as conn:
        rows = conn.execute(f"""
            SELECT
                n.firma,
                n.pozice,
                n.kraj,
                n.obor,
                n.plat_text,
                n.plat_od,
                n.plat_do,
                n.url,
                n.pocet_scanu,
                n.datum_prvni_scan,
                n.datum_posledni_scan,
                n.publikovano,
                n.predchozi_job_id,
                n.predchozi_datum_prvni,
                CAST(JULIANDAY('now') - JULIANDAY(n.datum_prvni_scan) AS INTEGER) AS dni_aktivni
            FROM nabidky n
            WHERE n.aktivni = 1 {na}
            ORDER BY n.firma, n.pozice
        """).fetchall()

    result = []
    for r in rows:
        row = dict(r)
        # Computed booleans for Looker Studio filters
        row["je_aktualizovano"] = 1 if (row.get("publikovano") or "") and "ktualizov" in row["publikovano"] else 0
        row["je_opakovane"] = 1 if row.get("predchozi_job_id") else 0
        row["je_problematicke"] = 1 if row.get("pocet_scanu", 0) >= 4 else 0
        row["je_starnouci"] = 1 if (row.get("dni_aktivni") or 0) >= 21 else 0
        row["ma_plat"] = 1 if row.get("plat_od") and row["plat_od"] > 0 else 0
        row["plat_stred"] = int((row["plat_od"] + row["plat_do"]) / 2) if row.get("plat_od") and row.get("plat_do") else 0
        # Clean date columns for Sheets
        row["datum_prvni"] = (row.get("datum_prvni_scan") or "")[:10]
        row["datum_posledni"] = (row.get("datum_posledni_scan") or "")[:10]
        result.append(row)

    return result


if __name__ == "__main__":
    init_db()
    print(f"Databáze inicializována: {DB_PATH}")
    print(json.dumps(statistiky(), ensure_ascii=False, indent=2))
