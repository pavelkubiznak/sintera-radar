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
                   datum_vydani, aktivni, publikovano,
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

            # Enrich: decision maker availability (batch)
            norms = list({normalize_company(f) for f in firms})
            if norms:
                ph2 = ",".join("?" * len(norms))
                dm_norms_found = {r[0] for r in conn.execute(
                    f"SELECT DISTINCT company_normalized FROM decision_makers "
                    f"WHERE company_normalized IN ({ph2})", norms
                ).fetchall()}
                # Also check substring matches for firms not found exactly
                if len(dm_norms_found) < len(norms):
                    all_dm_norms = [r[0] for r in conn.execute(
                        "SELECT DISTINCT company_normalized FROM decision_makers "
                        "WHERE current_company != ''"
                    ).fetchall()]
                    for n in norms:
                        if n in dm_norms_found:
                            continue
                        if len(n) >= 4:
                            for dn in all_dm_norms:
                                if len(dn) >= 4 and (n in dn or dn in n):
                                    dm_norms_found.add(n)
                                    break

                for p in positions:
                    p["has_dm"] = normalize_company(p["firma"]) in dm_norms_found

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


# ── MISSILE v4: Cold LinkedIn DM Generator ─────────────────────────────────
#
# 3-part structure:  FACT → INTERPRETATION → MICRO-STEP
#
# Priority system (strongest signal wins):
#   1. Regional competitive pressure (4+ firms, same role cluster, same region)
#   2. Rare skill combination (3+ uncommon requirements in title)
#   3. Hiring wave (5+ positions at once)
#   4. Missing salary when competitors show it
#   5. Long-running role (6+ weeks)
#   → INSUFFICIENT_DATA if none apply → skip, don't generate
#
# Rules:
#   - Opening: "Pane {VOCATIVE}," / "Paní {SURNAME}," — immediately the fact
#   - Signature: just "Šárka" on a new line
#   - Role names naturalized (no gender markers, no DB artifacts)
#   - Czech declension: proper case for role names in sentences
#   - BANNED: nabídka, klient, recruitment, agentura, headhunter, outsourcing,
#     pomoc, analýza, audit, vidím/všimla/zachytila, služba, spolupráce, konzultace,
#     kandidáti/síť kandidátů, Sintera, Dobrý den, Direct Search
#   - 3–4 sentences max.  Goal: "jak to ví?" → reply.


# ── Role naturalization ───────────────────────────────────────────

_GENDER_RE = re.compile(
    r'(?:\s*/\s*(?:t)?ka\b|\s*\*\s*ka\b|\s*/\s*[čc]ka\b'
    r'|\s*/\s*pracovnice\b'
    r'|\s*\(m/[žz](?:/n)?\))',
    re.IGNORECASE,
)
_SALARY_FRAG_RE = re.compile(
    r'\s*[-–—]\s*(?:až|od|do)?\s*\d[\d\s]*(?:Kč|CZK|EUR)\b.*$',
    re.IGNORECASE,
)
_CITIES = [
    'Praha', 'Brno', 'Ostrava', 'Plzeň', 'Olomouc', 'Liberec',
    'České Budějovice', 'Hradec Králové', 'Karlovy Vary', 'Zlín',
    'Pardubice', 'Jihlava', 'Ústí nad Labem', 'Opava', 'Most',
    'Frýdek-Místek', 'Karviná', 'Chomutov', 'Děčín', 'Teplice',
    'Kladno', 'Mladá Boleslav', 'Beroun', 'Kolín',
]


def _naturalize_role(raw: str, region: str = "") -> str:
    """Raw job board title → natural conversational nominative form.

    'Obchodní manažer/ka – technický facility management (m/ž)'
    → 'obchodní manažer pro facility management'
    """
    s = raw.strip()
    # 1. Gender markers
    s = _GENDER_RE.sub('', s)
    # 1b. Full feminine alternative: "operátor/operátorka", "Specialista/Specialistka"
    def _keep_masc(match):
        w1, w2 = match.group(1), match.group(2)
        prefix = 0
        for a, b in zip(w1.lower(), w2.lower()):
            if a == b:
                prefix += 1
            else:
                break
        return w1 if prefix >= 3 else match.group(0)
    s = re.sub(r'(\w{4,})/(\w{4,})\b', _keep_masc, s)
    # 2. Salary fragments
    s = _SALARY_FRAG_RE.sub('', s)
    # 3. Location redundancy
    if region:
        s = re.sub(r'\b' + re.escape(region) + r'\b', '', s, flags=re.IGNORECASE)
    for city in _CITIES:
        s = re.sub(r'(?<!\w)' + re.escape(city) + r'(?!\w)', '', s,
                   flags=re.IGNORECASE)
    # 4. Internal codes + parenthetical junk + marketing phrases
    s = re.sub(r'\b(?:Req|ID|Ref)[-\s]*\d[\d-]*\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\s*\([^)]{5,}\)', '', s)  # strip long parentheticals
    s = re.sub(r'\s+v\s+srdci\s+.*$', '', s, flags=re.IGNORECASE)  # "v srdci..."
    s = re.sub(r'\s+[-–—]\s+přidej\s+se\s.*$', '', s, flags=re.IGNORECASE)
    # 5. Separator → "pro" (en-dash, em-dash, or spaced hyphen)
    s = re.sub(r'\s*[–—]\s*', ' pro ', s, count=1)
    s = re.sub(r'\s+-\s+', ' pro ', s, count=1)
    s = re.sub(r'\bpro\s+pro\b', 'pro', s)
    # 6. Trailing junk
    s = re.sub(r'\s*-\s*$', '', s)
    s = re.sub(r'^\s*-\s*', '', s)
    s = re.sub(r'\s{2,}', ' ', s).strip()
    # 7. Lowercase
    #   - ALL CAPS words >4 chars → full lowercase (shouted regular words)
    #   - ALL CAPS 2-4 chars → keep (acronyms: IT, PLC, SAP)
    #   - Mixed case → lowercase first letter only (preserve brand casing)
    #   - Slash-separated tokens (PLC/SCADA) processed per-part
    words = s.split()
    out = []
    for w in words:
        parts = w.split('/')
        cased_parts = []
        for part in parts:
            core = part.strip('(),;:.')
            if core.isupper() and len(core) >= 2 and core.isalpha():
                if len(core) <= 5:
                    cased_parts.append(part)          # IT, PLC, SAP, SCADA — keep
                else:
                    cased_parts.append(part.lower())   # PROGRAMÁTOR → programátor
            else:
                cased_parts.append(part[0].lower() + part[1:] if part else part)
        out.append('/'.join(cased_parts))
    s = ' '.join(out)
    # 8. Trim trailing punctuation
    s = s.strip().rstrip('.,;:–—- ')
    return s


# ── Czech declension (accusative / genitive) ─────────────────────

_NO_DECLINE = frozenset([
    'senior', 'junior', 'lead', 'chief', 'head', 'data', 'full', 'front',
    'back', 'end', 'stack', 'product', 'project', 'quality', 'business',
    'account', 'key', 'site', 'facility', 'management', 'field', 'service',
    'banking', 'development', 'delivery', 'support', 'sales', 'marketing',
    'pro', 'na', 'v', 've', 'a', 'se',
])


def _decline_word_acc(word: str) -> str:
    """Best-effort accusative/genitive of a single Czech word.
    Returns the word unchanged if declension is uncertain."""
    if not word or len(word) < 3:
        return word
    low = word.lower()
    if low in _NO_DECLINE:
        return word
    if word.isupper() and word.isalpha():
        return word  # acronym

    # -- DON'T decline: verbal nouns, neuter nouns, foreign words --
    # Verbal nouns: -ování, -ávání, -ení, -ání, -utí
    if re.search(r'(?:ování|ávání|ení|ání|utí|ství|ctví)$', low):
        return word
    # Known neuter/indeclinable patterns
    if low.endswith("um") or low.endswith("io"):  # centrum, studio
        return word

    # -- Adjectives --
    # -ní (procesní, stavební) → -ního
    if low.endswith("ní") and not low.endswith("ání") and not low.endswith("ení"):
        return word + "ho"
    if low.endswith("cí") or low.endswith("čí"):
        return word + "ho"
    if low.endswith("í"):
        return word[:-1] + "ího"
    if low.endswith("ý"):
        return word[:-1] + "ého"

    # -- Masculine animate nouns --
    if low.endswith("ista"):
        return word[:-1] + "u"         # specialista → specialistu
    if low.endswith("ce"):
        return word                     # správce stays (soft paradigm)
    for suf in ("ér", "er", "ýr", "or", "ór", "ik", "ík", "ant", "ent",
                "ekt", "ect", "át", "ot"):
        if low.endswith(suf):
            return word + "a"
    if low.endswith("tel"):
        return word + "e"               # ředitel → ředitele
    if low.endswith("eč"):
        return word + "e"               # svářeč → svářeče
    if low.endswith("éř") or low.endswith("ář"):
        return word + "e"               # bankéř → bankéře, elektrikář → elektrikáře

    # Default: DON'T guess — return unchanged
    return word


def _decline_role_acc(role_nom: str) -> str:
    """Decline full role name to accusative/genitive.

    'procesní inženýr' → 'procesního inženýra'
    'IT analytik pro bankovnictví' → 'IT analytika pro bankovnictví'
    'specialista výzkumu a inovací' → 'specialistu výzkumu a inovací'

    Only the first 1-2 words (adjective + main noun) are declined.
    Everything after: prepositions, genitives, descriptors — stays unchanged.
    """
    # Split at preposition
    parts = re.split(r'(\s+(?:pro|na|v|ve|se|s)\s+)', role_nom, maxsplit=1)
    core = parts[0]
    rest = ''.join(parts[1:]) if len(parts) > 1 else ''

    words = core.split()
    if not words:
        return role_nom

    # Find the main noun position: skip modifiers like "senior", acronyms
    _SKIP = {'senior', 'junior', 'lead', 'head', 'chief', 'key'}
    declined = []
    noun_found = False
    for i, w in enumerate(words):
        wl = w.lower().strip('(),')
        if not noun_found:
            if wl in _SKIP or (w.isupper() and w.isalpha() and len(w) <= 4):
                # Modifier or acronym — keep, don't decline
                declined.append(w)
            else:
                # This is the (adjective or) noun — decline it
                declined.append(_decline_word_acc(w))
                # Check if NEXT word is the actual noun (this was an adjective)
                if i + 1 < len(words) and wl.endswith(("ní", "cí", "čí", "ý", "í")):
                    continue  # adjective — noun is next
                noun_found = True
        else:
            # After main noun — don't decline further
            declined.append(w)
    return ' '.join(declined) + rest


# ── Czech vocative ────────────────────────────────────────────────

def _vocative_male(surname: str) -> str:
    """Best-effort Czech vocative of a male surname.
    Novák → Nováku, Jireček → Jirečku, Koudelka → Koudelko.
    Adjectival -ský/-cký stay unchanged."""
    s = surname.strip()
    low = s.lower()
    if (low.endswith("ský") or low.endswith("cký") or
            low.endswith("ný") or low.endswith("ý")):
        return s
    if low.endswith("ek"):
        return s[:-2] + "ku"
    if low.endswith("ák"):
        return s + "u"
    if low.endswith("ík"):
        return s + "u"
    if low.endswith("ec"):
        return s[:-2] + "če"
    if low.endswith("a") and not low.endswith("ová"):
        return s[:-1] + "o"
    return s                             # fallback: nominative


# ── Gender detection + oslovení ───────────────────────────────────

def _gender_osloveni(dm: dict) -> str:
    """'Paní Nováková' / 'Pane Nováku'. Czech gender with vocative for males."""
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
    # Strip trailing titles: ", MBA", etc.
    clean = re.sub(
        r'[,\s]+(MBA|PhD|CSc|Ing|Mgr|Bc|DiS|Dr|Ph\.?D\.?)\s*\.?\s*$',
        '', last, flags=re.IGNORECASE).strip().rstrip(',').strip()
    if not clean or len(clean) <= 1:
        return dm.get("full_name", "").strip()
    if clean.islower() or clean.isupper():
        clean = clean.title()
    low = clean.lower()
    # Female surname patterns
    if (low.endswith("ová") or low.endswith("ova")
            or low.endswith("ská") or low.endswith("ska")
            or low.endswith("cká") or low.endswith("cka")
            or (low.endswith("á") and len(clean) > 2)):
        return "Paní {}".format(clean)
    # Fallback: female first name
    if first:
        fl = first.lower().rstrip('.')
        if fl.endswith("a") or fl.endswith("ie") or fl.endswith("na"):
            return "Paní {}".format(clean)
    return "Pane {}".format(_vocative_male(clean))


# ── Days computation ──────────────────────────────────────────────

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


# ── Role clustering ───────────────────────────────────────────────

_ROLE_KW_ORDER = [
    # longer / more specific first
    'obchodní zástupce', 'obchodní manažer', 'obchodní konzultant',
    'key account', 'account manager', 'sales manager',
    'projektový manažer', 'project manager', 'product manager',
    'procesní inženýr', 'stavební technik', 'servisní technik',
    'finanční účetní', 'vedoucí směny', 'vedoucí oddělení',
    'vedoucí výroby', 'kontrolor kvality', 'operátor výroby',
    'technik údržby', 'sociální pracovník', 'zdravotní sestra',
    'business development', 'IT specialista', 'HR specialista',
    'data analyst', 'data engineer', 'data scientist',
    'full stack', 'front end', 'back end',
    # single-word nouns
    'inženýr', 'manažer', 'technik', 'analytik', 'programátor',
    'operátor', 'projektant', 'konzultant', 'specialista',
    'koordinátor', 'developer', 'tester', 'architekt', 'konstruktér',
    'ředitel', 'asistent', 'účetní', 'vedoucí', 'správce',
    'administrátor', 'designer', 'mechanik', 'elektrikář',
    'svářeč', 'řidič', 'skladník', 'nákupčí', 'ekonom',
    'referent', 'scientist', 'engineer', 'manager', 'analyst',
    'controller', 'obchodník', 'prodejce', 'fyzioterapeut',
    'stavbyvedoucí',
]


def _extract_role_cluster(title: str) -> str:
    """Extract role cluster key for regional competition grouping."""
    t = title.lower()
    t = re.sub(r'[/\*]\s*ka\b', '', t)
    t = re.sub(r'\(m/[žz](?:/n)?\)', '', t)
    for kw in _ROLE_KW_ORDER:
        if kw in t:
            return kw
    words = t.split()
    skip = {'senior', 'junior', 'lead', 'hlavní', 'samostatný'}
    for w in words:
        if w not in skip and len(w) > 3:
            return w
    return ""


# ── Region locative ──────────────────────────────────────────────

def _region_locative(region: str) -> str:
    """'Jihomoravský kraj' → 'Jihomoravském kraji' (for 'v …')."""
    r = region.strip()
    _MAP = {
        'Praha': 'Praze',
        'Celá ČR': 'Česku',
        'Kraj Vysočina': 'kraji Vysočina',
    }
    if r in _MAP:
        return _MAP[r]
    if r.endswith('ský kraj'):
        return r[:-8] + 'ském kraji'
    if r.endswith('cký kraj'):
        return r[:-8] + 'ckém kraji'
    # bare adjective without "kraj"
    if r.endswith('ský'):
        return r[:-3] + 'ském kraji'
    if r.endswith('cký'):
        return r[:-3] + 'ckém kraji'
    return 'regionu ' + r


# ── Number words ─────────────────────────────────────────────────

_CZ_NUMS = {
    2: 'dvě', 3: 'tři', 4: 'čtyři', 5: 'pět', 6: 'šest', 7: 'sedm',
    8: 'osm', 9: 'devět', 10: 'deset', 11: 'jedenáct', 12: 'dvanáct',
}


def _num_cz(n: int) -> str:
    return _CZ_NUMS.get(n, str(n))


# ── Firma shortening ─────────────────────────────────────────────

def _shorten_firma(firma: str) -> str:
    """Strip legal suffixes for natural DM use."""
    s = firma.strip()
    s = re.sub(
        r',?\s*(?:s\.r\.o\.|a\.s\.|a\.\s*s\.|SE|GmbH|AG|Ltd\.?|Inc\.?'
        r'|spol\.\s*s\s*r\.?o\.?|v\.o\.s\.)\s*$',
        '', s, flags=re.IGNORECASE,
    )
    return s.strip().rstrip(',').strip()


# ── Pre-compute competitive context ──────────────────────────────

def _missile_context() -> dict:
    """Pre-compute regional competition + salary transparency data.
    Called once per generate_missile_dms() invocation.
    Returns {'competition': {(kraj, cluster): set_of_firms},
             'salary': {(kraj, cluster): (total, with_salary)} }.
    """
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT firma, kraj, pozice, plat_od "
            "FROM nabidky WHERE aktivni = 1 AND is_agency = 0"
        ).fetchall()

    competition = {}      # (kraj, cluster) → set of firma (lowercased)
    salary = {}           # (kraj, cluster) → [total, with_salary]

    for r in rows:
        cl = _extract_role_cluster(r["pozice"])
        if not cl:
            continue
        key = (r["kraj"], cl)
        # competition
        if key not in competition:
            competition[key] = set()
        competition[key].add(r["firma"].lower().strip())
        # salary
        if key not in salary:
            salary[key] = [0, 0]
        salary[key][0] += 1
        if r["plat_od"] and r["plat_od"] > 0:
            salary[key][1] += 1

    return {"competition": competition, "salary": salary}


# ── Priority determination ────────────────────────────────────────

_P2_LANG = re.compile(
    r'\b(?:angli[čc]|n[ěe]m[čc]|fran[čc]|english|german|french)\b', re.I)
_P2_TECH = re.compile(
    r'\b(?:SAP|Siemens|PLC|CNC|CAD|AutoCAD|Python|Java|\.NET'
    r'|C\+\+|SQL|AWS|Azure|Kubernetes|Docker|React|Angular'
    r'|Salesforce|Oracle|ABAP|S7|TIA)\b', re.I)
_P2_DOMAIN = re.compile(
    r'\b(?:automotive|pharma|energetik|strojírenstv|elektro'
    r'|bankovnictv|pojišťovnictv|logistik|chemick)\b', re.I)
_P2_SENIOR = re.compile(
    r'\b(?:senior|lead|vedoucí|head\s+of|ředitel|principal)\b', re.I)

# Generic clusters that don't make good P1 signals on their own
_P1_GENERIC = frozenset([
    'specialista', 'manažer', 'technik', 'vedoucí', 'asistent',
    'referent', 'koordinátor', 'konzultant', 'manager', 'engineer',
    'analyst', 'obchodní zástupce', 'obchodní manažer',
])


def _missile_priority(m: dict, ctx: dict, kraj_filter: str) -> tuple:
    """Determine strongest signal for a company.
    Returns (priority_int, signal_text, extra_dict).
    priority=0 means INSUFFICIENT_DATA → skip.

    Priority evaluation order follows spec (1→5), but each priority
    has strict activation criteria so not every company triggers P1.
    """
    pozice = m.get("pozice", [])
    if not pozice:
        return (0, "", {})

    firma_low = m["firma"].lower().strip()
    total_pozic = len(pozice)
    competition = ctx["competition"]
    salary = ctx["salary"]

    # ── Priority 1: Regional competitive pressure ──────────────────
    # Strict criteria:
    #   - Specific region only (not "Celá ČR")
    #   - 2-word cluster minimum (1-word like "specialista" is too broad)
    #   - 4–12 competitors (>12 = too common to be impressive)
    #   - Cluster must not be in the generic blacklist
    best_p1 = None
    best_p1_n = 0
    for p in pozice:
        p_kraj = p.get("kraj", "") or ""
        if not p_kraj or p_kraj == "Celá ČR":
            continue
        cl = _extract_role_cluster(p["pozice"])
        if not cl or cl in _P1_GENERIC:
            continue
        key = (p_kraj, cl)
        if key in competition:
            n = len(competition[key] - {firma_low})
            if 4 <= n <= 12 and n > best_p1_n:
                best_p1 = p
                best_p1_n = n
                best_p1_kraj = p_kraj
                best_p1_cl = cl
    if best_p1 is not None:
        return (1, f"{best_p1_n} firms in {best_p1_kraj} hiring {best_p1_cl}", {
            "role": best_p1["pozice"],
            "competitors": best_p1_n,
            "cluster": best_p1_cl,
            "region": best_p1_kraj,
        })

    # ── Priority 2: Rare skill combination ────────────────────────
    for p in pozice:
        title = p["pozice"]
        cats = sum([
            bool(_P2_LANG.search(title)),
            bool(_P2_TECH.search(title)),
            bool(_P2_DOMAIN.search(title)),
            bool(_P2_SENIOR.search(title)),
        ])
        if cats >= 3:
            combo_parts = []
            lm = _P2_LANG.search(title)
            if lm:
                combo_parts.append(lm.group(0).lower())
            tm = _P2_TECH.search(title)
            if tm:
                combo_parts.append(tm.group(0))
            dm2 = _P2_DOMAIN.search(title)
            if dm2:
                combo_parts.append(dm2.group(0).lower())
            combo = " + ".join(combo_parts[:3])
            p_kraj = p.get("kraj", "") or ""
            return (2, f"Rare combo: {combo}", {
                "role": p["pozice"],
                "combo": combo,
                "region": p_kraj,
            })

    # ── Priority 3: Hiring wave (5+ positions) ────────────────────
    if total_pozic >= 5:
        return (3, f"{total_pozic} open positions", {
            "count": total_pozic,
        })

    # ── Priority 4: Missing salary when competitors show it ───────
    for p in pozice:
        plat = p.get("plat_od") or 0
        if plat > 0:
            continue
        p_kraj = p.get("kraj", "") or ""
        cl = _extract_role_cluster(p["pozice"])
        if not cl:
            continue
        key = (p_kraj, cl)
        if key in salary:
            total, with_sal = salary[key]
            if total >= 5 and with_sal / total >= 0.50:
                return (4, f"{with_sal}/{total} ads with salary in {p_kraj}/{cl}", {
                    "role": p["pozice"],
                    "region": p_kraj,
                    "with_salary": with_sal,
                    "total_ads": total,
                })

    # ── Priority 5: Long-running role (42+ days = 6+ weeks) ──────
    for p in pozice:
        dni = _compute_dni(p)
        if dni >= 42:
            weeks = dni // 7
            return (5, f"Running {weeks} weeks", {
                "role": p["pozice"],
                "weeks": weeks,
                "region": p.get("kraj", "") or "",
            })

    # ── INSUFFICIENT_DATA ─────────────────────────────────────────
    return (0, "", {})


# ── Message generation ────────────────────────────────────────────

_STRONG_CTAS = [
    "Pošlete mi číslo, řeknu vám, jak to vidím.",
    "Pošlete mi číslo, řeknu za dvě minuty, "
    "jestli bych to ještě zkoušel takhle, nebo už jinak.",
    "Pošlete mi číslo, řeknu rovnou, jestli má cenu ještě čekat.",
]
_SOFT_CTA = "Má smysl si na to dát dvě minuty po telefonu?"


def _pick_cta(firma_norm: str, strong: bool = True) -> str:
    """Deterministic CTA selection per company."""
    if not strong:
        return _SOFT_CTA
    v = sum(ord(c) * (i + 1) for i, c in enumerate(firma_norm or "x"))
    return _STRONG_CTAS[v % len(_STRONG_CTAS)]


def _missile_message(priority: int, osloveni: str,
                     role_nom: str, role_acc: str,
                     firma: str, firma_norm: str,
                     region: str, extra: dict) -> str:
    """Generate the DM.  3 sentences: FACT → INTERPRETATION → CTA.
    Signature: 'Šárka' on its own line.
    """
    loc = _region_locative(region)

    if priority == 1:
        n = extra["competitors"]
        cta = _pick_cta(firma_norm, strong=True)
        return (
            f"{osloveni}, {role_acc} teď v {loc} hledá "
            f"kromě vás dalších {_num_cz(n)} firem. "
            f"V takové situaci obvykle nevyhrává lepší inzerát, "
            f"ale rychlost k pasivním kandidátům. "
            f"{cta}\n\nŠárka"
        )

    if priority == 2:
        combo = extra["combo"]
        cta = _pick_cta(firma_norm, strong=True)
        return (
            f"{osloveni}, {role_nom} s {combo} — to je kombinace, "
            f"kterou v {loc} aktivně hledá málo lidí. "
            f"Většina z nich práci nemění přes inzeráty. "
            f"{cta}\n\nŠárka"
        )

    if priority == 3:
        pocet = extra["count"]
        cta = _pick_cta(firma_norm, strong=True)
        if pocet > 15:
            n_str = f"přes {_num_cz(pocet // 5 * 5)}"
        else:
            n_str = _num_cz(pocet)
        return (
            f"{osloveni}, u vás teď běží "
            f"{n_str} otevřených pozic najednou. "
            f"Při takovém objemu už většinou nejde o to, "
            f"jestli přijdou správná CV — jde o kapacitu celého procesu. "
            f"{cta}\n\nŠárka"
        )

    if priority == 4:
        w = extra["with_salary"]
        t = extra["total_ads"]
        cta = _pick_cta(firma_norm, strong=False)
        return (
            f"{osloveni}, u {role_acc} v {loc} "
            f"teď {w} z {t} inzerátů uvádí mzdu. Váš ne. "
            f"Kandidát klikne nejdřív tam, kde číslo vidí, "
            f"a sem se vrátí až jako poslední — pokud vůbec. "
            f"{cta}\n\nŠárka"
        )

    if priority == 5:
        weeks = extra["weeks"]
        cta = _pick_cta(firma_norm, strong=True)
        return (
            f"{osloveni}, pozice {role_acc} u vás běží "
            f"přes {_num_cz(weeks)} týdnů. "
            f"V téhle fázi už aktivní kandidáti váš inzerát "
            f"viděli a nezareagovali. "
            f"{cta}\n\nŠárka"
        )

    return ""  # should not happen (priority 0 is filtered out)


# ── Validation ────────────────────────────────────────────────────

_BANNED_RE = re.compile(
    r'(?:'
    r'vidím,?\s+(?:že|u\s+vás)'
    r'|všimla\s+jsem|zachytila\s+jsem|narazila\s+jsem'
    r'|dívám\s+se|sleduju|prošla\s+jsem'
    r'|pracuju\s+s|pomáhám\s+firmám'
    r'|ozývám\s+se'
    r'|ráda\s+bych|chtěla\s+bych'
    r'|napíšu\s+rovnou|napíšu\s+stručně'
    r'|\bagentur[ay]?\b|\bpersonální\b'
    r'|\bslužb[auy]?\b|\bnabídk[auy]?\b|\břešení\b'
    r'|\bkandidát[ůyi]?\b|\bsíť\s+(?:lidí|kandidátů)\b'
    r'|\bpomoc\b|\bpomůžeme\b'
    r'|\bspolupráce?\b|\bspolupracovat\b'
    r'|\bkonzultac[ei]?\b|\baudit\b|\banalýz[auy]?\b'
    r'|\bdirect\s+search\b|\bsintera\b'
    r'|\bvím,?\s+jak\s+(?:to\s+frustruje|moc)\b'
    r'|\bchápu,?\s+že\b'
    r'|\bdávalo\s+by\s+smysl\b'
    r'|\bkdyby\s+vás\s+to\s+zajímalo\b'
    r'|\bpokud\s+to\s+dává\s+smysl\b'
    r'|\bráda\s+se\s+propojím\b'
    r'|\bdobrý\s+den\b|\bvážen[ýá]\b'
    r')',
    re.IGNORECASE,
)


def _missile_validate(msg: str) -> bool:
    """Return True if message passes all validation checks."""
    if not msg:
        return False
    # Must start with Pane/Paní
    if not msg.startswith("Pan"):
        return False
    # Must end with Šárka
    if "Šárka" not in msg:
        return False
    # No banned words
    if _BANNED_RE.search(msg):
        return False
    # Sentence count: 3-4 (rough check: count periods + question marks)
    body = msg.split("\n\n")[0]  # before signature
    sents = len(re.findall(r'[.?!]', body))
    if sents > 5:
        return False
    return True


# ── UI constants ──────────────────────────────────────────────────

_PRIORITY_LABELS = {
    1: "Regionální konkurence",
    2: "Vzácná kombinace",
    3: "Budují tým",
    4: "Chybí plat",
    5: "Dlouho otevřená",
    0: "Nedostatek dat",
}

_PRIORITY_COLORS = {
    1: ("#dc2626", "#fef2f2"),
    2: ("#7c3aed", "#faf5ff"),
    3: ("#2563eb", "#dbeafe"),
    4: ("#d97706", "#fefce8"),
    5: ("#ea580c", "#fff7ed"),
    0: ("#64748b", "#f8fafc"),
}


# ── Aktualizované DM template (conversational) ──────────────────


def _shorten_firma(name: str) -> str:
    """Strip legal suffixes for natural company name.
    'Škoda Auto a.s.' → 'Škoda Auto', 'ABB s.r.o.' → 'ABB'."""
    result = re.sub(
        r'[,\s]+(?:'
        r'a\.?\s*s\.?'
        r'|s\.?\s*r\.?\s*o\.?'
        r'|spol\.\s*s\s*r\.?\s*o\.?'
        r'|SE|k\.?\s*s\.?|v\.?\s*o\.?\s*s\.?'
        r'|z\.?\s*s\.?|z\.?\s*ú\.?|o\.?\s*p\.?\s*s\.?'
        r'|příspěvková\s+organizace'
        r')\s*\.?\s*$',
        '', name, flags=re.IGNORECASE
    ).strip().rstrip(',').strip()
    return result if result else name


def _prep_ve(word: str) -> str:
    """Return 've' or 'v' preposition per Czech phonetic rules."""
    w = word.strip().lower()
    if not w:
        return 'v'
    # "ve" only before consonant clusters: v/f/s/š/z/ž + consonant
    vowels = set('aeiouyáéíóúůý')
    if len(w) >= 2 and w[0] in ('v', 'f', 's', 'š', 'z', 'ž') and w[1] not in vowels:
        return 've'
    return 'v'


def _detect_gender(dm: dict) -> str:
    """Detect gender from DM contact. Returns 'm' or 'f'."""
    last = dm.get("last_name", "").strip()
    first = dm.get("first_name", "").strip()
    if not last:
        full = dm.get("full_name", "").strip()
        parts = full.split()
        if len(parts) >= 2:
            last = parts[-1]
            first = parts[0] if not first else first
    clean = re.sub(
        r'[,\s]+(MBA|PhD|CSc|Ing|Mgr|Bc|DiS|Dr|Ph\.?D\.?)\s*\.?\s*$',
        '', last, flags=re.IGNORECASE).strip().rstrip(',').strip()
    low = clean.lower()
    if (low.endswith("ová") or low.endswith("ova")
            or low.endswith("ská") or low.endswith("ska")
            or low.endswith("cká") or low.endswith("cka")
            or (low.endswith("á") and len(clean) > 2)):
        return 'f'
    if first:
        fl = first.lower().rstrip('.')
        if fl.endswith("a") or fl.endswith("ie") or fl.endswith("na"):
            return 'f'
    return 'm'


# Skill keywords: (regex_pattern, genitive_form, category)
_SKILL_KW = [
    # Tech
    (r'\bSAP\b', 'SAPu', 'tech'),
    (r'\bPLC\b', 'PLC', 'tech'),
    (r'\bS7\b', 'S7', 'tech'),
    (r'\bCNC\b', 'CNC', 'tech'),
    (r'\bERP\b', 'ERP', 'tech'),
    (r'\bMES\b', 'MES', 'tech'),
    (r'\bSCADA\b', 'SCADA', 'tech'),
    (r'\bHMI\b', 'HMI', 'tech'),
    (r'\bTIA\s+Portal\b', 'TIA Portalu', 'tech'),
    (r'\bPython\b', 'Pythonu', 'tech'),
    (r'\bJava\b', 'Javy', 'tech'),
    (r'\.NET\b', '.NETu', 'tech'),
    (r'\bC#\b', 'C#', 'tech'),
    (r'\bC\+\+\b', 'C++', 'tech'),
    (r'\bSQL\b', 'SQL', 'tech'),
    (r'\bOracle\b', 'Oracle', 'tech'),
    (r'\bCAD\b', 'CADu', 'tech'),
    (r'\bAutoCAD\b', 'AutoCADu', 'tech'),
    (r'\bSolidWorks?\b', 'SolidWorks', 'tech'),
    (r'\bInventor\b', 'Inventoru', 'tech'),
    (r'\bRevit\b', 'Revitu', 'tech'),
    (r'\bBIM\b', 'BIM', 'tech'),
    (r'\bLinux\b', 'Linuxu', 'tech'),
    (r'\bAWS\b', 'AWS', 'tech'),
    (r'\bAzure\b', 'Azure', 'tech'),
    (r'\bDocker\b', 'Dockeru', 'tech'),
    (r'\bKubernetes\b', 'Kubernetes', 'tech'),
    (r'\bReact\b', 'Reactu', 'tech'),
    (r'\bAngular\b', 'Angularu', 'tech'),
    (r'\bPHP\b', 'PHP', 'tech'),
    (r'\bABAP\b', 'ABAPu', 'tech'),
    (r'\bSiemens\b', 'Siemens', 'tech'),
    (r'\bFanuc\b', 'Fanucu', 'tech'),
    (r'\bHeidenhain\b', 'Heidenhain', 'tech'),
    # Languages (prefix match — catches all Czech declensions)
    (r'\bangličtin\w*', 'angličtiny', 'lang'),
    (r'\banglick', 'angličtiny', 'lang'),
    (r'\bněmčin\w*', 'němčiny', 'lang'),
    (r'\bněmeck', 'němčiny', 'lang'),
    (r'\bfrancouzštin\w*', 'francouzštiny', 'lang'),
    # Domain
    (r'\bautomotive\b', 'automotive', 'domain'),
    (r'\bpharma\b', 'pharma', 'domain'),
    (r'\benergetik[auy]?\b', 'energetiky', 'domain'),
    (r'\bstavebnictv[ií]?\b', 'stavebnictví', 'domain'),
    (r'\blogistik[auy]?\b', 'logistiky', 'domain'),
    (r'\belektro\b', 'elektro', 'domain'),
    (r'\bstrojírenstv\w*\b', 'strojírenství', 'domain'),
    (r'\bpotravinářstv\w*\b', 'potravinářství', 'domain'),
    (r'\bfinance?\b', 'financí', 'domain'),
    # Skills / functions
    (r'\bkvalit[auy]?\b|\bkvalitář\b', 'kvality', 'skill'),
    (r'\bkonstrukc[eí]\b|\bkonstruktér\b', 'konstrukce', 'skill'),
    (r'\btechnolog\w*\b', 'technologie', 'skill'),
    (r'\búčet\w*\b', 'účetnictví', 'skill'),
    (r'\bcontrolling\b', 'controllingu', 'skill'),
    (r'\bnákup\w*\b', 'nákupu', 'skill'),
    (r'\bobchod\w*\b|\bsales\b', 'obchodu', 'skill'),
    (r'\bmarketing\b', 'marketingu', 'skill'),
    (r'\bBOZP\b', 'BOZP', 'skill'),
    (r'\bbezpečnost\w*\b', 'bezpečnosti', 'skill'),
    (r'\bprojekc[eí]\b', 'projekce', 'skill'),
    (r'\búdržb[auy]\b', 'údržby', 'skill'),
    (r'\bvýrob[auy]?\b', 'výroby', 'skill'),
    (r'\bse[rř][ií]z\w*\b', 'seřizování', 'skill'),
    (r'\bsvařov\w*\b', 'svařování', 'skill'),
    (r'\bobráb\w*\b', 'obrábění', 'skill'),
    (r'\bskladov\w*\b', 'skladování', 'skill'),
    (r'\bexpedic\w*\b', 'expedice', 'skill'),
    # Level / condition
    (r'\bsenior\b', 'seniority', 'level'),
    (r'\bvedoucí\b|\bvedení\b', 'vedení lidí', 'level'),
    (r'\bteam\s*lead\b', 'vedení týmu', 'level'),
    (r'\bsměn\w*\b', 'směnného provozu', 'condition'),
    (r'\bcertifikac\w*\b', 'certifikací', 'condition'),
    (r'\bvyhláš\w*\b', 'vyhlášky', 'condition'),
    (r'\bzákaznick\w*\b', 'zákaznického kontaktu', 'condition'),
]


def _extract_skill_combo(title: str) -> str:
    """Extract a 2-factor skill combination from position title.
    Returns 'kombinace X a Y' or empty string if <2 categories found."""
    # Expand slash-separated terms: "PLC/SCADA" → "PLC SCADA"
    expanded = re.sub(r'/', ' ', title)

    found = []  # (genitive_form, category)
    seen_cats = set()

    for pattern, gen_form, cat in _SKILL_KW:
        if cat in seen_cats:
            continue
        if re.search(pattern, expanded, re.IGNORECASE):
            found.append((gen_form, cat))
            seen_cats.add(cat)
            if len(found) >= 2:
                break

    if len(found) >= 2:
        return "kombinace {} a {}".format(found[0][0], found[1][0])
    return ""


_BANNED_AKT_RE = re.compile(
    r'(?:'
    r'vidím,?\s+(?:že|u\s+vás)'
    r'|všimla\s+jsem|zachytila\s+jsem'
    r'|prošla\s+jsem\s+si|dívám\s+se|sleduju'
    r'|pracuju\s+s|pomáhám\s+firmám'
    r'|ozývám\s+se'
    r'|ráda\s+bych|chtěla\s+bych'
    r'|\bagentur[ay]?\b|\bpersonální\b'
    r'|\bslužb[auy]?\b|\bnabídk[auy]?\b|\břešení\b'
    r'|\bkandidát[ůyi]?\b|\bsíť\s+(?:lidí|kandidátů)\b'
    r'|\bpomoc\b|\bpomůžeme\b'
    r'|\bspolupráce?\b|\bspolupracovat\b'
    r'|\bkonzultac[ei]?\b|\baudit\b|\banalýz[auy]?\b'
    r'|\bdirect\s+search\b|\bsintera\b'
    r'|\bpošlete\s+mi\s+číslo\b'
    r'|\bmám\s+pro\s+vás\b'
    r'|\bviděla\s+jsem,?\s+že\b'
    r'|\bzdarma\b'
    r')',
    re.IGNORECASE,
)


def _missile_message_aktualizovano(dm_contact: dict, pozice_title: str,
                                    firma: str, combo: str) -> str:
    """Generate conversational DM for aktualizované positions.
    Light, human, no sales pitch. Max 3 sentences."""
    # Oslovení: "Dobrý den, pane Nováku," / "Dobrý den, paní Nováková,"
    osloveni_raw = _gender_osloveni(dm_contact)
    if not osloveni_raw or len(osloveni_raw) < 5:
        return ""
    # _gender_osloveni returns "Pane Nováku" / "Paní Nováková" → lowercase for mid-sentence
    osloveni = "Dobrý den, " + osloveni_raw[0].lower() + osloveni_raw[1:]

    gender = _detect_gender(dm_contact)
    chtela = "chtěla" if gender == 'f' else "chtěl"

    firma_short = _shorten_firma(firma)
    prep = _prep_ve(firma_short)

    role = _naturalize_role(pozice_title, "")

    # Combo sentence
    if combo:
        combo_sent = ("Podobné role teď vídám častěji a vím, "
                      "že {} bývá dost často oříšek.".format(combo))
    else:
        combo_sent = ("Podobné role teď vídám častěji a vím, "
                      "že podobně postavené role bývají dost často oříšek.")

    msg = (
        "{osloveni}, náhodou jsem narazila na pozici {role}, "
        "kterou u vás {prep} {firma} znovu aktualizovali. "
        "{combo} "
        "Kdybyste {chtela}, můžu Vám ukázat, co se v takové chvíli dá udělat, "
        "aby se to s náborem zbytečně netáhlo."
    ).format(
        osloveni=osloveni,
        role=role,
        prep=prep,
        firma=firma_short,
        combo=combo_sent,
        chtela=chtela,
    )
    return msg


def _validate_aktualizovano(msg: str) -> bool:
    """Validate message for aktualizované template."""
    if not msg or not msg.startswith("Dobrý den"):
        return False
    if _BANNED_AKT_RE.search(msg):
        return False
    body = msg.strip()
    sents = len(re.findall(r'[.?!]', body))
    return sents <= 4


# ── Single-position DM generator (for pozice page) ──────────────


def generate_dm_for_position(firma: str, pozice_title: str = "",
                              kraj: str = ""):
    """Generate a DM for a specific company + position.
    Looks up decision maker, generates conversational message.
    Returns dict with kontakt info + message, or None if no DM found."""
    firma_norm = normalize_company(firma)

    # Find decision maker for this company
    with get_conn() as conn:
        dm_rows = conn.execute(
            "SELECT * FROM decision_makers WHERE company_normalized = ?",
            (firma_norm,)
        ).fetchall()

    # Try substring match if exact match fails
    if not dm_rows:
        with get_conn() as conn:
            all_dms = conn.execute(
                "SELECT * FROM decision_makers WHERE current_company != ''"
            ).fetchall()
        for dm in all_dms:
            dn = dm["company_normalized"]
            if len(dn) >= 4 and len(firma_norm) >= 4:
                if firma_norm in dn or dn in firma_norm:
                    dm_rows = [dm]
                    break

    if not dm_rows:
        return None

    dm_contact = dict(dm_rows[0])

    # Validate contact
    last = dm_contact.get("last_name", "").strip()
    full = dm_contact.get("full_name", "").strip()
    if len(full) < 5 or len(last) <= 2:
        return None

    # Generate message using aktualizované template (conversational)
    combo = _extract_skill_combo(pozice_title) if pozice_title else ""
    msg = _missile_message_aktualizovano(dm_contact, pozice_title, firma, combo)
    if not msg or not _validate_aktualizovano(msg):
        return None

    return {
        "kontakt": dm_contact.get("full_name", ""),
        "kontakt_title": dm_contact.get("current_title", ""),
        "linkedin_url": dm_contact.get("profile_url", ""),
        "firma": firma,
        "firma_norm": firma_norm,
        "pozice": pozice_title,
        "kraj": kraj,
        "zprava": msg,
        "char_count": len(msg),
    }


# ── Main MISSILE generator ───────────────────────────────────────

def generate_missile_dms(kraj: str = "", limit: int = 50,
                         flag: str = "", sort: str = "") -> list:
    """MISSILE v4 cold DM generator.

    FACT → INTERPRETATION → MICRO-STEP.
    Priority system: 1 (regional competition) → 2 (rare combo) →
    3 (hiring wave) → 4 (missing salary) → 5 (long-running).
    If no priority fires → INSUFFICIENT_DATA → skip.
    Naturalized role names, Czech declension, banned-word validation.

    flag: '' (all), 'aktualizovano', 'opakovane', 'problematicke'
          Filters to only companies with that flag.
    sort: '' (smart default), 'flags' (flagged first), 'priority', 'dni'
    """
    result = radar_matches(skip_fuzzy=True)
    matches = result.get("matches", [])
    ctx = _missile_context()

    dms = []
    for m in matches:
        if m.get("outreach_status"):
            continue
        if not m.get("decision_makers"):
            continue
        if kraj and kraj not in m.get("kraje", []):
            continue

        msg_kraj = kraj or (m["kraje"][0] if m.get("kraje") else "ČR")

        # ── Contact validation (shared) ──
        dm_contact = m["decision_makers"][0]
        last = dm_contact.get("last_name", "").strip()
        full = dm_contact.get("full_name", "").strip()
        if len(full) < 5 or len(last) <= 2:
            continue
        clean_last = re.sub(
            r'[,\s]+(MBA|PhD|CSc|Ing|Mgr|Bc|DiS).*$', '',
            last, flags=re.IGNORECASE).strip().rstrip(',')
        if clean_last and not any(c.isupper() for c in clean_last):
            continue

        # ── Position flags (shared) ──
        pozice_list = m.get("pozice", [])
        akt_pozice = [
            p for p in pozice_list
            if p.get("publikovano") and "ktualizov" in p["publikovano"]
        ]
        cnt_aktualizovano = len(akt_pozice)
        cnt_opakovane = sum(
            1 for p in pozice_list if p.get("predchozi_job_id")
        )
        cnt_problematicke = sum(
            1 for p in pozice_list if p.get("pocet_scanu", 0) >= 4
        )
        _today = date.today()
        max_dni = 0
        for p in pozice_list:
            dps = p.get("datum_prvni_scan", "")
            if dps:
                try:
                    d0 = date.fromisoformat(dps[:10])
                    days = (_today - d0).days
                    if days > max_dni:
                        max_dni = days
                except (ValueError, TypeError):
                    pass

        flags = []
        if cnt_aktualizovano:
            flags.append({"typ": "aktualizovano", "barva": "#dc2626",
                          "bg": "#fef2f2", "text": f"Aktualizováno ({cnt_aktualizovano})"})
        if cnt_opakovane:
            flags.append({"typ": "opakovane", "barva": "#7c3aed",
                          "bg": "#faf5ff", "text": f"Opakované ({cnt_opakovane})"})
        if cnt_problematicke:
            flags.append({"typ": "problematicke", "barva": "#ea580c",
                          "bg": "#fff7ed", "text": f"Problém ({cnt_problematicke})"})

        # ── Branch: aktualizované → conversational template, else P1-P5 ──
        if akt_pozice:
            # Pick best aktualizovaná position (highest pocet_scanu)
            best_akt = max(akt_pozice, key=lambda p: p.get("pocet_scanu", 0))
            combo = _extract_skill_combo(best_akt["pozice"])
            msg = _missile_message_aktualizovano(
                dm_contact, best_akt["pozice"], m["firma"], combo,
            )
            if not msg or not _validate_aktualizovano(msg):
                continue
            role_nom = _naturalize_role(best_akt["pozice"], "")
            pri = 0  # special — sorts before P1
            archetype = "AKT"
            archetype_label = "Aktualizovaná pozice"
            color, bg = "#dc2626", "#fef2f2"
        else:
            # ── P1-P5 priority system ──
            pri, signal_text, extra = _missile_priority(m, ctx, kraj)
            if pri == 0:
                continue  # INSUFFICIENT_DATA — skip

            osloveni = _gender_osloveni(dm_contact)
            if not osloveni or len(osloveni) < 5:
                continue
            ref_role_raw = extra.get("role", "")
            if not ref_role_raw:
                ref_role_raw = m["pozice"][0]["pozice"] if m.get("pozice") else ""
            role_region = extra.get("region", msg_kraj)
            role_nom = _naturalize_role(ref_role_raw, role_region)
            role_acc = _decline_role_acc(role_nom)
            msg = _missile_message(
                pri, osloveni, role_nom, role_acc,
                m["firma"], m["firma_norm"], role_region, extra,
            )
            if not msg or not _missile_validate(msg):
                continue
            archetype = f"P{pri}"
            archetype_label = _PRIORITY_LABELS.get(pri, "")
            color, bg = _PRIORITY_COLORS.get(pri, ("#64748b", "#f8fafc"))

        dms.append({
            "firma": m["firma"],
            "firma_norm": m["firma_norm"],
            "kontakt": dm_contact["full_name"],
            "kontakt_title": dm_contact.get("current_title", ""),
            "linkedin_url": dm_contact.get("profile_url", ""),
            "kraj": msg_kraj,
            "signal": m.get("signal", ""),
            "signal_score": m.get("signal_score", 0),
            "pocet_pozic": m["pocet_pozic"],
            "archetype": archetype,
            "archetype_label": archetype_label,
            "archetype_color": color,
            "archetype_bg": bg,
            "priority": pri,
            "pozice_ref": role_nom,
            "zprava": msg,
            "char_count": len(msg),
            "flags": flags,
            "max_dni": max_dni,
        })

    # ── Flag filter ──
    if flag:
        dms = [d for d in dms if any(f["typ"] == flag for f in d["flags"])]

    # ── Sorting ──
    def _flag_weight(d):
        """Higher = more actionable. aktualizovano=30, opakovane=20, problematicke=10."""
        w = 0
        for f in d["flags"]:
            if f["typ"] == "aktualizovano":
                w += 30
            elif f["typ"] == "opakovane":
                w += 20
            elif f["typ"] == "problematicke":
                w += 10
        return w

    if sort == "dni":
        dms.sort(key=lambda d: (-d["max_dni"], d["priority"]))
    elif sort == "priority":
        dms.sort(key=lambda d: (d["priority"], -d["signal_score"]))
    else:
        # Smart default: flagged companies first (aktualizovano > opakovane > problematicke),
        # then by priority within each tier
        dms.sort(key=lambda d: (-_flag_weight(d), d["priority"], -d["signal_score"]))

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
