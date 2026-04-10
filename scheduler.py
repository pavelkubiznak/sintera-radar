#!/usr/bin/env python3
"""
Automatický scheduler — spouští scraping každé pondělí a čtvrtek.

Spuštění (nechte běžet na pozadí):
  python scheduler.py

Nebo jako macOS služba (launchd):
  sudo cp com.sintera.jobsscraper.plist ~/Library/LaunchAgents/
  launchctl load ~/Library/LaunchAgents/com.sintera.jobsscraper.plist
"""

import logging
import os
import smtplib
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import schedule
import time

from database import init_db, uloz_nabidky, statistiky, exportuj_csv, radar_matches, dm_statistiky
from scraper import scrape_vsechny_kraje, OBORY

# ── Konfigurace ────────────────────────────────────────────────────────────────

LOG_PATH = Path(__file__).parent / "data" / "scheduler.log"
LOG_PATH.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# Načti nastavení z prostředí / .env
from dotenv import load_dotenv
load_dotenv()

EMAIL_OD      = os.getenv("EMAIL_OD", "")
EMAIL_HESLO   = os.getenv("EMAIL_HESLO", "")
EMAIL_KOMU    = os.getenv("EMAIL_KOMU", "info@sintera.cz")
SMTP_SERVER   = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))

# Výchozí obory (z obrázku) — lze přepsat v .env jako čárkami oddělený seznam
VYCHOZI_OBORY_STR = os.getenv(
    "OBORY",
    "strojirenstvi,technika,elektrotechnika,vyroba,veda,chemie,"
    "doprava,farmacie,ekonomika,kvalita,remesla,nakup,hr,management,"
    "telekomunikace,zakaznicky"
)
VYCHOZI_OBORY = [o.strip() for o in VYCHOZI_OBORY_STR.split(",") if o.strip()]

PRIMA_ZAMESTNAVATELE = os.getenv("PRIMA_ZAMESTNAVATELE", "false").lower() == "true"


# ── Scraping úloha ────────────────────────────────────────────────────────────

def spust_scraping() -> dict:
    log.info("=" * 60)
    log.info("Spouštím scraping všech krajů ČR")
    log.info(f"Obory: {', '.join(VYCHOZI_OBORY)}")
    log.info(f"Přímí zaměstnavatelé: {PRIMA_ZAMESTNAVATELE}")
    log.info("=" * 60)

    start = datetime.now()
    init_db()

    try:
        nabidky = scrape_vsechny_kraje(
            obory=VYCHOZI_OBORY,
            prima_zamestnavatele=PRIMA_ZAMESTNAVATELE,
            verbose=True,
        )
        log.info(f"Staženo celkem: {len(nabidky)} nabídek")

        # Ulož do DB
        stats = uloz_nabidky([n.__dict__ for n in nabidky])
        log.info(f"DB: {stats['nove']} nových, {stats['aktualizovane']} aktualizovaných")

        # Exportuj CSV
        csv_path = exportuj_csv()
        log.info(f"CSV export: {csv_path}")

        elapsed = (datetime.now() - start).total_seconds()
        log.info(f"Hotovo za {elapsed:.0f} sekund")

        return {**stats, "celkem_stazeno": len(nabidky), "elapsed": elapsed, "chyba": None}

    except Exception as e:
        log.error(f"Chyba při scrapingu: {e}", exc_info=True)
        return {"nove": 0, "aktualizovane": 0, "celkem": 0, "chyba": str(e)}


# ── E-mail report ─────────────────────────────────────────────────────────────

def _sestav_html(stats_db: dict, run_stats: dict) -> str:
    ok = not run_stats.get("chyba")
    status_blok = (
        f"<div style='background:#d1fae5;border-left:4px solid #059669;"
        f"padding:12px 16px;border-radius:6px;margin-bottom:1rem;font-size:15px'>"
        f"✅ <strong>Sběr dat proběhl úspěšně</strong> — "
        f"{run_stats.get('nove', 0)} nových, "
        f"{run_stats.get('aktualizovane', 0)} aktualizovaných, "
        f"{run_stats.get('opakovani', 0)} opakovaných zadání</div>"
        if ok else
        f"<div style='background:#fee2e2;border-left:4px solid #dc2626;"
        f"padding:12px 16px;border-radius:6px;margin-bottom:1rem;font-size:15px'>"
        f"❌ <strong>Sběr dat SELHAL</strong> — {run_stats.get('chyba', 'neznámá chyba')}</div>"
    )

    top_firmy_rows = "".join(
        f"<tr><td>{f['firma']}</td><td>{f['pocet_inzeratu']}</td>"
        f"<td>{f['max_scanu']}</td></tr>"
        for f in stats_db.get("top_firmy", [])
    )
    dlouho_rows = "".join(
        f"<tr><td><a href='{n['url']}'>{n['pozice']}</a></td>"
        f"<td>{n['firma']}</td><td>{n['kraj']}</td>"
        f"<td style='color:#dc2626;font-weight:700'>{n['pocet_scanu']}×</td></tr>"
        for n in stats_db.get("dlouho_aktivni", [])[:10]
    )
    kraje_rows = "".join(
        f"<tr><td>{k['kraj']}</td><td>{k['pocet']}</td></tr>"
        for k in stats_db.get("kraje_stats", [])
    )
    opakovane_rows = "".join(
        f"<tr><td><a href='{n['url']}'>{n['pozice']}</a></td>"
        f"<td>{n['firma']}</td><td>{n['kraj']}</td>"
        f"<td style='font-size:12px;color:#666'>{n['predchozi_datum_prvni'][:10] if n.get('predchozi_datum_prvni') else '—'}</td>"
        f"<td style='font-size:12px;color:#666'>{n['datum_prvni_scan'][:10] if n.get('datum_prvni_scan') else '—'}</td></tr>"
        for n in stats_db.get("opakovane_pozice", [])
    )

    opakovane_sekce = (
        f"<h2>🔁 Opakovaně vypsané pozice (firma zkusila znovu)</h2>"
        f"<table><tr><th>Pozice</th><th>Firma</th><th>Kraj</th>"
        f"<th>1. pokus od</th><th>2. pokus od</th></tr>"
        f"{opakovane_rows}</table>"
        if opakovane_rows else ""
    )

    return f"""<!DOCTYPE html>
<html lang="cs"><head><meta charset="UTF-8">
<style>
  body {{ font-family: Arial, sans-serif; font-size: 14px; color: #222; max-width: 900px; margin: 0 auto; padding: 20px; }}
  h1 {{ color: #0052cc; margin-bottom: .5rem; }}
  h2 {{ color: #333; border-bottom: 1px solid #ddd; padding-bottom: 4px; margin: 1.5rem 0 .75rem; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 1rem; }}
  th {{ background: #0052cc; color: #fff; padding: 6px 10px; text-align: left; font-size:13px; }}
  td {{ padding: 5px 10px; border-bottom: 1px solid #eee; font-size:13px; }}
  tr:hover td {{ background: #f5f8ff; }}
  .stat {{ display: inline-block; background: #e8f0fe; border-radius: 8px;
           padding: 10px 18px; margin: 4px; text-align: center; }}
  .stat strong {{ display: block; font-size: 1.5em; color: #0052cc; }}
  .stat span {{ font-size: 11px; color: #555; }}
  a {{ color: #0052cc; }}
</style>
</head><body>
<h1>📊 Jobs.cz Report — {datetime.now().strftime('%d.%m.%Y %H:%M')}</h1>
{status_blok}

<div style="margin-bottom:1.25rem">
  <div class="stat"><strong>{run_stats.get('nove', 0)}</strong><span>Nové inzeráty</span></div>
  <div class="stat"><strong>{run_stats.get('aktualizovane', 0)}</strong><span>Aktualizované</span></div>
  <div class="stat"><strong>{run_stats.get('opakovani', 0)}</strong><span>Opakovaná zadání</span></div>
  <div class="stat"><strong>{stats_db.get('aktivni', 0)}</strong><span>Celkem aktivních</span></div>
  <div class="stat"><strong>{stats_db.get('total', 0)}</strong><span>V databázi celkem</span></div>
</div>

<h2>🏆 Top firmy — počet aktivních inzerátů</h2>
<table>
  <tr><th>Firma</th><th>Počet inzerátů</th><th>Max. opakování</th></tr>
  {top_firmy_rows if top_firmy_rows else '<tr><td colspan="3" style="color:#999">Žádná data</td></tr>'}
</table>

{opakovane_sekce}

<h2>⏰ Pozice aktivní 4+ skenování — firma pravděpodobně neobsadí sama</h2>
<table>
  <tr><th>Pozice</th><th>Firma</th><th>Kraj</th><th>Počet skenů</th></tr>
  {dlouho_rows if dlouho_rows else '<tr><td colspan="4" style="color:#999">Zatím žádné</td></tr>'}
</table>

<h2>📍 Aktivní nabídky po krajích</h2>
<table>
  <tr><th>Kraj</th><th>Aktivních nabídek</th></tr>
  {kraje_rows if kraje_rows else '<tr><td colspan="2" style="color:#999">Žádná data</td></tr>'}
</table>

<p style="color:#bbb;font-size:11px;margin-top:1.5rem;border-top:1px solid #eee;padding-top:.75rem">
  Doba sběru: {run_stats.get('elapsed', 0):.0f} s &nbsp;·&nbsp;
  Poslední scan: {stats_db.get('posledni_scan', '—')} &nbsp;·&nbsp;
  Sintera Jobs Intelligence &nbsp;·&nbsp;
  <a href="http://127.0.0.1:5000/databaze">Otevřít databázi</a>
</p>
</body></html>"""


def _sestav_radar_html() -> str:
    """Sestaví HTML sekci radaru pro e-mail."""
    result = radar_matches()
    dm_stats = dm_statistiky()

    if dm_stats["total"] == 0:
        return ""  # No decision makers loaded, skip radar section

    matches = result["matches"]
    if not matches:
        return (
            "<h2>🎯 SINTERA RADAR</h2>"
            "<p style='color:#888'>Decision makers nahrani ({} kontaktu), "
            "ale zadna firma se neshoduje s aktivnimi inzeraty. "
            "Zkontrolujte nazvy firem.</p>".format(dm_stats["total"])
        )

    # Split into signal categories
    high = [m for m in matches if m["signal_score"] >= 15]
    watch = [m for m in matches if 8 <= m["signal_score"] < 15]

    def _match_row(m):
        dms = m["decision_makers"]
        dm_text = ", ".join(
            "{} ({})".format(d["full_name"], d["current_title"])
            for d in dms[:2]
        )
        pozice_text = ", ".join(p["pozice"] for p in m["pozice"][:3])
        if len(m["pozice"]) > 3:
            pozice_text += " +{}".format(len(m["pozice"]) - 3)

        signal_color = "#dc2626" if m["signal_score"] >= 15 else "#ea580c"
        return (
            "<tr>"
            "<td><strong>{}</strong></td>"
            "<td>{}</td>"
            "<td style='font-size:12px'>{}</td>"
            "<td><span style='background:#fee2e2;color:{};padding:2px 8px;"
            "border-radius:10px;font-size:11px;font-weight:600'>{}</span></td>"
            "<td style='font-size:12px'>{}</td>"
            "</tr>".format(
                m["firma"], ", ".join(m["kraje"]), pozice_text,
                signal_color, m["signal"], dm_text
            )
        )

    rows_high = "".join(_match_row(m) for m in high[:10])
    rows_watch = "".join(_match_row(m) for m in watch[:10])

    sections = "<h2>🎯 SINTERA RADAR — {} shod</h2>".format(len(matches))

    if rows_high:
        sections += (
            "<h3 style='color:#dc2626;margin:12px 0 6px'>🔴 Vysoky signal — jednat tento tyden</h3>"
            "<table><tr><th>Firma</th><th>Region</th><th>Pozice</th>"
            "<th>Signal</th><th>Kontakt</th></tr>"
            "{}</table>".format(rows_high)
        )

    if rows_watch:
        sections += (
            "<h3 style='color:#ea580c;margin:12px 0 6px'>🟡 Sledovat</h3>"
            "<table><tr><th>Firma</th><th>Region</th><th>Pozice</th>"
            "<th>Signal</th><th>Kontakt</th></tr>"
            "{}</table>".format(rows_watch)
        )

    sections += (
        "<p style='font-size:12px;color:#888;margin-top:8px'>"
        "Celkem {} propojených firem z {} firem s aktivnimi inzeraty. "
        "{} decision makeru v {} regionech. "
        "<a href='http://127.0.0.1:5001/radar'>Otevrit Radar</a></p>".format(
            result["stats"]["matched"],
            result["stats"]["total_jobs_companies"],
            dm_stats["total"],
            len(dm_stats["per_kraj"]),
        )
    )

    return sections


def posli_email(run_stats: dict) -> bool:
    if not EMAIL_OD or not EMAIL_HESLO:
        log.warning("E-mail není nakonfigurován (EMAIL_OD / EMAIL_HESLO v .env)")
        return False

    stats_db = statistiky()
    html = _sestav_html(stats_db, run_stats)

    # Insert radar section before closing </body>
    radar_html = _sestav_radar_html()
    if radar_html:
        html = html.replace("</body>", radar_html + "</body>")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = (
        f"Jobs.cz Report {datetime.now().strftime('%d.%m.%Y')} — "
        f"{run_stats.get('nove', 0)} nových, {stats_db.get('aktivni', 0)} aktivních"
    )
    msg["From"] = EMAIL_OD
    msg["To"]   = EMAIL_KOMU
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_OD, EMAIL_HESLO)
            server.sendmail(EMAIL_OD, EMAIL_KOMU, msg.as_string())
        log.info(f"E-mail odeslán na {EMAIL_KOMU}")
        return True
    except Exception as e:
        log.error(f"Chyba při odesílání e-mailu: {e}")
        return False


# ── Kombinovaná úloha ─────────────────────────────────────────────────────────

def ulohа_scraping_a_email():
    run_stats = spust_scraping()
    posli_email(run_stats)


# ── Plánování ─────────────────────────────────────────────────────────────────

def main():
    log.info("Scheduler spuštěn")
    log.info("Plán: každé pondělí a čtvrtek v 06:00")

    # Každé pondělí v 5:00
    schedule.every().monday.at("05:00").do(ulohа_scraping_a_email)
    # Každý čtvrtek v 5:00
    schedule.every().thursday.at("05:00").do(ulohа_scraping_a_email)

    # Volitelně: spustit hned při startu pro test
    if "--hned" in sys.argv:
        log.info("--hned: spouštím okamžitě")
        ulohа_scraping_a_email()

    log.info("Čekám na plánovaný čas... (Ctrl+C pro ukončení)")
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
