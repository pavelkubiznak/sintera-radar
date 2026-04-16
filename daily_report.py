"""
Daily Sintera Radar Report
Runs full scrape, analyzes intelligence, sends email report.
"""

import os
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import date, timedelta, datetime
from pathlib import Path

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).parent))

from database import init_db, get_conn, normalize_company, detect_surge, uloz_nabidky
from scraper import scrape, scrape_vsechny_kraje, KRAJE


# ── Config ───────────────────────────────────────────────────────────────────

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
REPORT_TO = os.environ.get("REPORT_TO", SMTP_USER)
SERVER_URL = os.environ.get("SERVER_URL", "http://178.104.207.97:5001")


# ── Scrape all regions ───────────────────────────────────────────────────────

def run_full_scrape() -> dict:
    """Scrape all 14 regions, all industries. Returns aggregate stats + per-region."""
    init_db()
    totals = {"nove": 0, "aktualizovane": 0, "opakovani": 0, "celkem": 0}
    per_region = {}  # kraj_name -> {"pocet": N, "novych": N, "chyba": ""}
    dnes = date.today().isoformat()
    den_tydne = date.today().weekday()  # 0=Mon, 6=Sun

    for kraj_kod, (kraj_name, kraj_slug) in KRAJE.items():
        print(f"[{datetime.now():%H:%M:%S}] Scraping {kraj_name}...")
        region_info = {"pocet": 0, "novych": 0, "chyba": ""}
        try:
            nabidky = scrape(
                kraj_slug=kraj_slug, kraj_nazev=kraj_name,
                obory=[], verbose=False,
                prima_zamestnavatele=True,
            )
            if nabidky:
                stats = uloz_nabidky([n.__dict__ for n in nabidky])
                for k in totals:
                    totals[k] += stats.get(k, 0)
                region_info["pocet"] = len(nabidky)
                region_info["novych"] = stats.get("nove", 0)
                print(f"  -> {len(nabidky)} positions ({stats.get('nove', 0)} new)")
            else:
                print(f"  -> 0 positions")
        except Exception as e:
            region_info["chyba"] = str(e)
            print(f"  ERROR: {e}")

        per_region[kraj_name] = region_info

        # Log to scrape_log
        with get_conn() as conn:
            conn.execute("""
                INSERT INTO scrape_log (datum, den_tydne, kraj, pocet, novych, chyba)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (dnes, den_tydne, kraj_name, region_info["pocet"],
                  region_info["novych"], region_info["chyba"]))

    totals["per_region"] = per_region
    return totals


# ── Anomaly detection ────────────────────────────────────────────────────────

def detect_anomalies(scrape_stats: dict) -> list:
    """Compare today's scrape with recent history. Returns list of anomaly dicts."""
    anomalies = []
    dnes = date.today()
    den_tydne = dnes.weekday()
    is_weekend = den_tydne >= 5

    per_region = scrape_stats.get("per_region", {})
    today_total = sum(r["pocet"] for r in per_region.values())

    with get_conn() as conn:
        # Get average totals for same type of day (weekday vs weekend) over last 14 days
        if is_weekend:
            day_filter = "den_tydne >= 5"
            day_label = "weekend"
        else:
            day_filter = "den_tydne < 5"
            day_label = "weekday"

        lookback = (dnes - timedelta(days=28)).isoformat()
        today_str = dnes.isoformat()

        # Overall average per day (same weekday/weekend type)
        avg_row = conn.execute(f"""
            SELECT AVG(daily_total) as avg_total, COUNT(DISTINCT datum) as days
            FROM (
                SELECT datum, SUM(pocet) as daily_total
                FROM scrape_log
                WHERE datum >= ? AND datum < ? AND {day_filter}
                GROUP BY datum
            )
        """, (lookback, today_str)).fetchone()

        avg_total = avg_row["avg_total"] or 0
        history_days = avg_row["days"] or 0

        # Per-region averages
        region_avgs = {}
        rows = conn.execute(f"""
            SELECT kraj, AVG(pocet) as avg_pocet, COUNT(*) as cnt
            FROM scrape_log
            WHERE datum >= ? AND datum < ? AND {day_filter}
            GROUP BY kraj
        """, (lookback, today_str)).fetchall()
        for r in rows:
            region_avgs[r["kraj"]] = {"avg": r["avg_pocet"], "cnt": r["cnt"]}

    # Need at least 3 days of history to compare
    if history_days < 3:
        if history_days == 0:
            anomalies.append({
                "level": "info",
                "message": "Prvni scrape — zatim neni historie pro porovnani.",
            })
        return anomalies

    # Check 1: Total positions dropped significantly (>35%)
    if avg_total > 0:
        drop_pct = (avg_total - today_total) / avg_total * 100
        if drop_pct > 35:
            anomalies.append({
                "level": "critical",
                "message": (
                    f"Celkovy pocet pozic VYRAZNE KLESL: {today_total} dnes vs. "
                    f"prumer {avg_total:.0f} ({day_label}). "
                    f"Pokles {drop_pct:.0f}%. Mozna zmena na jobs.cz!"
                ),
            })
        elif drop_pct > 20:
            anomalies.append({
                "level": "warning",
                "message": (
                    f"Pocet pozic nize nez obvykle: {today_total} dnes vs. "
                    f"prumer {avg_total:.0f} ({day_label}). Pokles {drop_pct:.0f}%."
                ),
            })

    # Check 2: Total positions jumped significantly (>50%) — unlikely, may mean duplicates
    if avg_total > 0:
        jump_pct = (today_total - avg_total) / avg_total * 100
        if jump_pct > 50:
            anomalies.append({
                "level": "warning",
                "message": (
                    f"Neobvykly narust pozic: {today_total} dnes vs. "
                    f"prumer {avg_total:.0f} ({day_label}). Narust {jump_pct:.0f}%."
                ),
            })

    # Check 3: Per-region — any region returned 0 that normally has results
    for kraj_name, info in per_region.items():
        avg_info = region_avgs.get(kraj_name)
        if not avg_info or avg_info["cnt"] < 2:
            continue

        region_avg = avg_info["avg"]
        if info["pocet"] == 0 and region_avg > 10:
            anomalies.append({
                "level": "critical",
                "message": (
                    f"Region {kraj_name}: 0 pozic (prumer {region_avg:.0f}). "
                    f"Mozny problem s URL nebo zmena na jobs.cz!"
                ),
            })
        elif region_avg > 0:
            region_drop = (region_avg - info["pocet"]) / region_avg * 100
            if region_drop > 50 and region_avg > 20:
                anomalies.append({
                    "level": "warning",
                    "message": (
                        f"Region {kraj_name}: {info['pocet']} pozic vs. prumer "
                        f"{region_avg:.0f}. Pokles {region_drop:.0f}%."
                    ),
                })

    # Check 4: Any scrape errors
    errors = [(k, v["chyba"]) for k, v in per_region.items() if v["chyba"]]
    if errors:
        for kraj, err in errors:
            anomalies.append({
                "level": "critical",
                "message": f"Chyba pri scrapovani {kraj}: {err[:100]}",
            })

    return anomalies


# ── Intelligence queries ─────────────────────────────────────────────────────

def get_refreshed_positions(limit: int = 30) -> list:
    """Positions marked 'Aktualizováno' on jobs.cz — strongest buying signal.
    These are positions the company has been struggling to fill."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT firma, pozice, kraj, publikovano, datum_vydani,
                   datum_prvni_scan, pocet_scanu, url,
                   CAST(JULIANDAY('now') - JULIANDAY(datum_prvni_scan) AS INTEGER) AS dni_aktivni
            FROM nabidky
            WHERE aktivni = 1
              AND publikovano LIKE '%ktualizov%'
              AND firma != ''
            ORDER BY dni_aktivni DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def get_refreshed_companies() -> list:
    """Group refreshed positions by company — shows which companies struggle most."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT firma, kraj,
                   COUNT(*) as pocet_aktualizovanych,
                   GROUP_CONCAT(pozice, ' | ') as pozice_list,
                   MAX(CAST(JULIANDAY('now') - JULIANDAY(datum_prvni_scan) AS INTEGER)) as max_dni
            FROM nabidky
            WHERE aktivni = 1
              AND publikovano LIKE '%ktualizov%'
              AND firma != ''
            GROUP BY firma
            HAVING COUNT(*) >= 1
            ORDER BY pocet_aktualizovanych DESC, max_dni DESC
            LIMIT 20
        """).fetchall()
    return [dict(r) for r in rows]


def get_aging_positions(min_days: int = 30, limit: int = 20) -> list:
    """Positions open for 30+ days without being filled."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT firma, pozice, kraj, datum_prvni_scan, pocet_scanu, url,
                   CAST(JULIANDAY('now') - JULIANDAY(datum_prvni_scan) AS INTEGER) AS dni_aktivni
            FROM nabidky
            WHERE aktivni = 1
              AND CAST(JULIANDAY('now') - JULIANDAY(datum_prvni_scan) AS INTEGER) >= ?
              AND firma != ''
            ORDER BY dni_aktivni DESC
            LIMIT ?
        """, (min_days, limit)).fetchall()
    return [dict(r) for r in rows]


def get_scrape_summary() -> dict:
    """Overall stats for the report."""
    dnes = date.today().isoformat()
    week_ago = (date.today() - timedelta(days=7)).isoformat()

    with get_conn() as conn:
        total_active = conn.execute(
            "SELECT COUNT(*) FROM nabidky WHERE aktivni = 1"
        ).fetchone()[0]
        new_today = conn.execute(
            "SELECT COUNT(*) FROM nabidky WHERE DATE(datum_prvni_scan) = ?", (dnes,)
        ).fetchone()[0]
        new_week = conn.execute(
            "SELECT COUNT(*) FROM nabidky WHERE DATE(datum_prvni_scan) >= ?", (week_ago,)
        ).fetchone()[0]
        refreshed_total = conn.execute(
            "SELECT COUNT(*) FROM nabidky WHERE aktivni = 1 AND publikovano LIKE '%ktualizov%'"
        ).fetchone()[0]
        regions = conn.execute("""
            SELECT kraj, COUNT(*) as cnt
            FROM nabidky WHERE aktivni = 1 AND kraj != ''
            GROUP BY kraj ORDER BY cnt DESC
        """).fetchall()

    return {
        "total_active": total_active,
        "new_today": new_today,
        "new_week": new_week,
        "refreshed_total": refreshed_total,
        "regions": [dict(r) for r in regions],
    }


# ── Email builder ────────────────────────────────────────────────────────────

def build_html_report(scrape_stats: dict) -> tuple:
    """Build the full HTML email report. Returns (html, anomalies)."""
    summary = get_scrape_summary()
    surges = detect_surge(threshold=3)
    refreshed = get_refreshed_companies()
    aging = get_aging_positions(min_days=21)
    anomalies = detect_anomalies(scrape_stats)

    today_str = date.today().strftime("%d.%m.%Y")

    # ── HTML ─────────────────────────────────────────────────────────────
    html = f"""
    <html>
    <head>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
               color: #333; max-width: 700px; margin: 0 auto; padding: 20px; }}
        h1 {{ color: #1e1b4b; font-size: 22px; border-bottom: 2px solid #6366f1;
              padding-bottom: 8px; }}
        h2 {{ color: #4338ca; font-size: 16px; margin-top: 28px; }}
        .stats {{ display: flex; gap: 12px; flex-wrap: wrap; margin: 16px 0; }}
        .stat {{ background: #f1f5f9; border-radius: 8px; padding: 12px 16px;
                 min-width: 120px; }}
        .stat .num {{ font-size: 24px; font-weight: 700; color: #1e1b4b; }}
        .stat .label {{ font-size: 12px; color: #64748b; }}
        table {{ border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 13px; }}
        th {{ background: #1e1b4b; color: white; padding: 8px 10px; text-align: left; }}
        td {{ padding: 7px 10px; border-bottom: 1px solid #e2e8f0; }}
        tr:hover {{ background: #f8fafc; }}
        .badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px;
                  font-size: 11px; font-weight: 600; }}
        .badge-red {{ background: #fef2f2; color: #dc2626; }}
        .badge-orange {{ background: #fff7ed; color: #ea580c; }}
        .badge-blue {{ background: #eff6ff; color: #2563eb; }}
        .section-icon {{ margin-right: 6px; }}
        .footer {{ margin-top: 30px; padding-top: 16px; border-top: 1px solid #e2e8f0;
                   font-size: 12px; color: #94a3b8; }}
        a {{ color: #4338ca; }}
    </style>
    </head>
    <body>
    <h1>Sintera Radar &mdash; Denni report {today_str}</h1>

    <!-- Summary stats -->
    <div class="stats">
        <div class="stat">
            <div class="num">{summary['total_active']:,}</div>
            <div class="label">Aktivnich pozic</div>
        </div>
        <div class="stat">
            <div class="num">{scrape_stats.get('nove', 0)}</div>
            <div class="label">Novych dnes</div>
        </div>
        <div class="stat">
            <div class="num">{summary['refreshed_total']}</div>
            <div class="label">Aktualizovanych</div>
        </div>
        <div class="stat">
            <div class="num">{summary['new_week']}</div>
            <div class="label">Novych tento tyden</div>
        </div>
    </div>
    """

    # ── Anomaly warnings (if any) ───────────────────────────────────────
    critical_anomalies = [a for a in anomalies if a["level"] == "critical"]
    warning_anomalies = [a for a in anomalies if a["level"] == "warning"]

    if critical_anomalies or warning_anomalies:
        html += """
        <div style="background: #fef2f2; border: 2px solid #dc2626; border-radius: 8px;
                    padding: 16px; margin: 16px 0;">
            <h2 style="color: #dc2626; margin: 0 0 10px 0; font-size: 16px;">
                ⚠️ UPOZORNENI — Detekovany anomalie
            </h2>
        """
        for a in critical_anomalies:
            html += f'<p style="margin: 6px 0; color: #dc2626; font-weight: 600;">🔴 {a["message"]}</p>'
        for a in warning_anomalies:
            html += f'<p style="margin: 6px 0; color: #ea580c;">🟡 {a["message"]}</p>'
        html += """
            <p style="font-size: 12px; color: #64748b; margin-top: 10px;">
                Pokud se toto opakuje vice dni, pravdepodobne doslo ke zmene na jobs.cz
                a je treba upravit scraper.
            </p>
        </div>
        """

    # ── Section 1: Refreshed positions (HOTTEST) ────────────────────────
    html += """
    <h2><span class="section-icon">🔥</span> NEJVETSI PRILEZITOSTI &mdash; Aktualizovane pozice</h2>
    <p style="font-size:13px; color:#64748b; margin-bottom:10px;">
        Firmy, ktere aktualizovaly inzeraty = hledaji dlouho a nemohou najit. <strong>Nejsilnejsi signal pro osloveni.</strong>
    </p>
    """

    if refreshed:
        html += """<table>
        <tr><th>Firma</th><th>Kraj</th><th>Aktualizovanych</th><th>Pozice</th><th>Max dni</th></tr>"""
        for r in refreshed[:15]:
            pozice_short = r["pozice_list"]
            if len(pozice_short) > 80:
                pozice_short = pozice_short[:80] + "..."
            badge = "badge-red" if r["max_dni"] > 30 else "badge-orange"
            firma_norm = normalize_company(r["firma"])
            html += f"""<tr>
                <td><a href="{SERVER_URL}/firma/{firma_norm}">{r['firma']}</a></td>
                <td>{r['kraj']}</td>
                <td><strong>{r['pocet_aktualizovanych']}</strong></td>
                <td style="font-size:12px;">{pozice_short}</td>
                <td><span class="badge {badge}">{r['max_dni']}d</span></td>
            </tr>"""
        html += "</table>"
    else:
        html += "<p><em>Zadne aktualizovane pozice dnes.</em></p>"

    # ── Section 2: Surge alerts ─────────────────────────────────────────
    html += """
    <h2><span class="section-icon">📈</span> SURGE &mdash; Firmy s nahlym narustem pozic</h2>
    <p style="font-size:13px; color:#64748b; margin-bottom:10px;">
        Firmy, ktere pridaly 3+ novych pozic tento tyden. Rostou a potrebuji lidi.
    </p>
    """

    if surges:
        html += """<table>
        <tr><th>Firma</th><th>Kraj</th><th>Novych pozic</th><th>Pozice</th></tr>"""
        for s in surges[:10]:
            pozice_txt = " | ".join(s["pozice_list"][:3])
            if len(pozice_txt) > 80:
                pozice_txt = pozice_txt[:80] + "..."
            firma_norm = s["firma_norm"]
            html += f"""<tr>
                <td><a href="{SERVER_URL}/firma/{firma_norm}">{s['firma']}</a></td>
                <td>{s['kraj']}</td>
                <td><strong>{s['nove_pozice']}</strong></td>
                <td style="font-size:12px;">{pozice_txt}</td>
            </tr>"""
        html += "</table>"
    else:
        html += "<p><em>Zadne surge alerty tento tyden.</em></p>"

    # ── Section 3: Aging positions ──────────────────────────────────────
    html += """
    <h2><span class="section-icon">⏰</span> STARNOUCI POZICE &mdash; Otevrene 21+ dni</h2>
    <p style="font-size:13px; color:#64748b; margin-bottom:10px;">
        Pozice otevrene pres 3 tydny. Firma pravdepodobne nema kapacitu obsadit sama.
    </p>
    """

    if aging:
        html += """<table>
        <tr><th>Firma</th><th>Pozice</th><th>Kraj</th><th>Dni otevrena</th><th>Scanu</th></tr>"""
        for a in aging[:15]:
            badge = "badge-red" if a["dni_aktivni"] > 45 else "badge-orange"
            firma_norm = normalize_company(a["firma"])
            html += f"""<tr>
                <td><a href="{SERVER_URL}/firma/{firma_norm}">{a['firma']}</a></td>
                <td style="font-size:12px;">{a['pozice']}</td>
                <td>{a['kraj']}</td>
                <td><span class="badge {badge}">{a['dni_aktivni']}d</span></td>
                <td>{a['pocet_scanu']}</td>
            </tr>"""
        html += "</table>"
    else:
        html += "<p><em>Zadne starnouci pozice.</em></p>"

    # ── Section 4: Per-region breakdown ─────────────────────────────────
    html += """
    <h2><span class="section-icon">🗺️</span> Prehled podle regionu</h2>
    """
    if summary["regions"]:
        html += """<table><tr><th>Region</th><th>Aktivnich pozic</th></tr>"""
        for reg in summary["regions"]:
            html += f"<tr><td>{reg['kraj']}</td><td>{reg['cnt']}</td></tr>"
        html += "</table>"

    # ── Footer ──────────────────────────────────────────────────────────
    html += f"""
    <div class="footer">
        <p>
            <a href="{SERVER_URL}">Otevrit Sintera Radar dashboard</a> &bull;
            Scrape dokoncen: {datetime.now().strftime('%H:%M:%S')} UTC &bull;
            Nove: {scrape_stats.get('nove', 0)} | Aktualizovane: {scrape_stats.get('aktualizovane', 0)} |
            Opakovane: {scrape_stats.get('opakovani', 0)}
        </p>
    </div>
    </body></html>
    """

    return html, anomalies


# ── Email sender ─────────────────────────────────────────────────────────────

def send_report(html: str, anomalies: list = None) -> bool:
    """Send the HTML report via Gmail SMTP."""
    if not SMTP_USER or not SMTP_PASS:
        print("ERROR: SMTP_USER and SMTP_PASS env vars required.")
        print("Set them in /opt/sintera-radar/.env")
        return False

    # Add warning prefix to subject if anomalies detected
    has_critical = any(a["level"] == "critical" for a in (anomalies or []))
    has_warning = any(a["level"] == "warning" for a in (anomalies or []))
    prefix = ""
    if has_critical:
        prefix = "🔴 PROBLEM — "
    elif has_warning:
        prefix = "🟡 "

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"{prefix}Sintera Radar — Denni report {date.today().strftime('%d.%m.%Y')}"
    msg["From"] = f"Sintera Radar <{SMTP_USER}>"
    msg["To"] = REPORT_TO

    # Plain text fallback
    text_part = MIMEText(
        f"Sintera Radar denni report — {date.today().strftime('%d.%m.%Y')}\n\n"
        f"Otevri report v prohlizeci: {SERVER_URL}\n",
        "plain", "utf-8"
    )
    html_part = MIMEText(html, "html", "utf-8")

    msg.attach(text_part)
    msg.attach(html_part)

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        print(f"Report sent to {REPORT_TO}")
        return True
    except Exception as e:
        print(f"ERROR sending email: {e}")
        return False


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"=== Sintera Radar Daily Report — {date.today()} ===")
    print()

    # Step 1: Full scrape
    print("Step 1: Running full scrape...")
    scrape_stats = run_full_scrape()
    print(f"\nScrape complete: {scrape_stats}")
    print()

    # Step 2: Build report
    print("Step 2: Building intelligence report...")
    html, anomalies = build_html_report(scrape_stats)
    print(f"Report built ({len(html)} chars)")
    if anomalies:
        print(f"  ANOMALIES DETECTED: {len(anomalies)}")
        for a in anomalies:
            print(f"  [{a['level'].upper()}] {a['message']}")
    print()

    # Step 3: Send email
    print("Step 3: Sending email...")
    success = send_report(html, anomalies)

    if success:
        print("\nDone! Report sent successfully.")
    else:
        print("\nScrape done but email failed. Check SMTP config.")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
