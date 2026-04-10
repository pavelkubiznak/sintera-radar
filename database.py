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
        }
        for col, sql in migrations.items():
            if col not in existing_cols:
                conn.execute(sql)

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
                        datum_vydani = COALESCE(NULLIF(?, ''), datum_vydani)
                    WHERE job_id = ?
                """.format(increment), (now, n.get("plat", ""), plat_od, plat_do,
                      n.get("obor", ""), n.get("datum_vydani", ""), job_id))
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

                conn.execute("""
                    INSERT INTO nabidky
                        (job_id, pozice, firma, misto, kraj, kraj_slug,
                         plat_text, plat_od, plat_do, url, obor, datum_vydani,
                         datum_prvni_scan, datum_posledni_scan,
                         pocet_scanu, aktivni,
                         predchozi_job_id, predchozi_datum_prvni)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?)
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
                    now, now,
                    predchozi_job_id, predchozi_datum,
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
        where, params = [], []
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
    with get_conn() as conn:
        total       = conn.execute("SELECT COUNT(*) FROM nabidky").fetchone()[0]
        aktivni     = conn.execute("SELECT COUNT(*) FROM nabidky WHERE aktivni = 1").fetchone()[0]
        nove_dnes   = conn.execute(
            "SELECT COUNT(*) FROM nabidky WHERE DATE(datum_prvni_scan) = DATE('now')"
        ).fetchone()[0]
        opakovani_celkem = conn.execute(
            "SELECT COUNT(*) FROM nabidky WHERE predchozi_job_id IS NOT NULL"
        ).fetchone()[0]
        top_firmy   = conn.execute("""
            SELECT firma, COUNT(*) as pocet_inzeratu, MAX(pocet_scanu) as max_scanu
            FROM nabidky WHERE firma != '' AND aktivni = 1
            GROUP BY firma ORDER BY pocet_inzeratu DESC LIMIT 10
        """).fetchall()
        dlouho_aktivni = conn.execute("""
            SELECT * FROM nabidky WHERE aktivni = 1 AND pocet_scanu >= 4
            ORDER BY pocet_scanu DESC LIMIT 20
        """).fetchall()
        posledni_scan = conn.execute(
            "SELECT hodnota FROM konfigurace WHERE klic = 'posledni_scan'"
        ).fetchone()
        kraje_stats = conn.execute("""
            SELECT kraj, COUNT(*) as pocet FROM nabidky WHERE aktivni = 1
            GROUP BY kraj ORDER BY pocet DESC
        """).fetchall()
        opakovane_pozice = conn.execute("""
            SELECT n.firma, n.pozice, n.kraj,
                   n.datum_prvni_scan, n.predchozi_datum_prvni,
                   n.url, n.job_id
            FROM nabidky n
            WHERE n.predchozi_job_id IS NOT NULL AND n.aktivni = 1
            ORDER BY n.datum_prvni_scan DESC LIMIT 15
        """).fetchall()

    return {
        "total": total,
        "aktivni": aktivni,
        "nove_dnes": nove_dnes,
        "opakovani_celkem": opakovani_celkem,
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


def radar_matches(min_scanu: int = 1, min_score: float = 0.82) -> list:
    """Hlavní funkce radaru.
    Najde firmy z jobs.cz, které mají odpovídajícího decision makera.
    Vrací seznam matchů seřazený podle signálu (počet pozic, délka inzerce).

    Optimalizováno pro rychlost: exact match a substring match jsou O(1) per firma.
    Fuzzy match (pomalý) se spouští jen na firmy s 3+ pozicemi, které nebyly nalezeny jinak.
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
            WHERE aktivni = 1 AND firma != ''
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
    unmatched_valuable = [
        (norm, data) for norm, data in jobs_by_company.items()
        if norm not in matched_job_norms and len(data["pozice"]) >= 3
    ]
    if unmatched_valuable and dm_norms:
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
    result = radar_matches()
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
    """Kompletní přehled o firmě — pozice, kontakty, outreach historie."""
    with get_conn() as conn:
        # All positions (active + inactive) for this company
        pozice = conn.execute("""
            SELECT job_id, pozice, kraj, obor, plat_text, url,
                   pocet_scanu, datum_prvni_scan, datum_posledni_scan,
                   datum_vydani, aktivni,
                   predchozi_job_id, predchozi_datum_prvni
            FROM nabidky
            WHERE firma != '' AND ? IN (
                SELECT firma_norm FROM (
                    SELECT job_id, ? as firma_norm FROM nabidky LIMIT 1
                )
            )
            ORDER BY aktivni DESC, pocet_scanu DESC
        """, (firma_norm, firma_norm)).fetchall()

        # Better: search by normalized company name
        # First find the original firma name
        all_firms = conn.execute(
            "SELECT DISTINCT firma FROM nabidky WHERE firma != ''"
        ).fetchall()

        target_firma = None
        for row in all_firms:
            if normalize_company(row["firma"]) == firma_norm:
                target_firma = row["firma"]
                break

        if not target_firma:
            return None

        pozice = [dict(r) for r in conn.execute("""
            SELECT job_id, pozice, kraj, obor, plat_text, url,
                   pocet_scanu, datum_prvni_scan, datum_posledni_scan,
                   datum_vydani, aktivni,
                   predchozi_job_id, predchozi_datum_prvni
            FROM nabidky
            WHERE firma = ?
            ORDER BY aktivni DESC, datum_vydani DESC, pocet_scanu DESC
        """, (target_firma,)).fetchall()]

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

        # Days since oldest active position
        max_days = 0
        for p in aktivni_pozice:
            if p.get("datum_vydani"):
                try:
                    d = date.fromisoformat(p["datum_vydani"])
                    days = (date.today() - d).days
                    if days > max_days:
                        max_days = days
                except ValueError:
                    pass

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
        }


# ── Dashboard stats ──────────────────────────────────────────────────────────

def dashboard_stats() -> dict:
    """Agregované statistiky pro dashboard."""
    week_ago = (date.today() - timedelta(days=7)).isoformat()

    with get_conn() as conn:
        aktivni = conn.execute("SELECT COUNT(*) FROM nabidky WHERE aktivni = 1").fetchone()[0]
        total_db = conn.execute("SELECT COUNT(*) FROM nabidky").fetchone()[0]
        nove_tyden = conn.execute(
            "SELECT COUNT(*) FROM nabidky WHERE DATE(datum_prvni_scan) >= ?", (week_ago,)
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
        "dm_total": dm_total,
        "outreach_total": outreach_total,
        "outreach_ceka": outreach_ceka,
        "outreach_odpoved": outreach_odpoved,
        "posledni_scan": posledni_scan[0] if posledni_scan else "—",
        "recent_outreach": recent_outreach,
    }


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

def radar_doporuceni(per_region: int = 3) -> list:
    """Top N firem za každý region, bez již oslovených.
    Vrací flat list seřazený: region → signal_score DESC.
    """
    result = radar_matches()
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


if __name__ == "__main__":
    init_db()
    print(f"Databáze inicializována: {DB_PATH}")
    print(json.dumps(statistiky(), ensure_ascii=False, indent=2))
