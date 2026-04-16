#!/usr/bin/env python3
"""Flask web GUI pro jobs.cz scraper s live progress barem a databázovým přehledem."""

import csv
import dataclasses
import io
import json
import os
import threading
import uuid
from datetime import datetime
from functools import wraps

from flask import Flask, render_template, request, Response, jsonify, redirect, url_for
from scraper import scrape, scrape_vsechny_kraje, KRAJE, OBORY, UVAZKY, HOME_OFFICE
from database import (init_db, uloz_nabidky, nacti_nabidky, statistiky, exportuj_csv,
                       import_linkedin_csv, add_decision_maker, dm_statistiky,
                       nacti_decision_makers, radar_matches, outreach_brief,
                       add_company_alias, add_outreach, update_outreach_status,
                       nacti_outreach, outreach_statistiky, OUTREACH_STATUSES,
                       radar_doporuceni, detect_surge, generate_batch_messages,
                       normalize_company, firma_detail, dashboard_stats,
                       filtr_pozice, firmy_prehled,
                       generate_missile_dms, salary_benchmark, analytics_export_data)

app = Flask(__name__)
init_db()

# Register normalize_company as Jinja filter so templates can build correct URLs
app.jinja_env.filters["normalize"] = normalize_company

# ── Basic Authentication ────────────────────────────────────────────────────
AUTH_USER = os.environ.get("AUTH_USER", "")
AUTH_PASS = os.environ.get("AUTH_PASS", "")


def check_auth(username, password):
    return username == AUTH_USER and password == AUTH_PASS


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not AUTH_USER:  # No auth configured = no protection (local dev)
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return Response(
                "Pristup odepren. Zadejte prihlasovaci udaje.",
                401,
                {"WWW-Authenticate": 'Basic realm="Sintera Radar"'},
            )
        return f(*args, **kwargs)
    return decorated


@app.before_request
def auth_guard():
    """Protect all routes with basic auth when configured."""
    if not AUTH_USER:
        return  # No auth configured
    if request.endpoint and request.endpoint == "static":
        return  # Allow static files
    auth = request.authorization
    if not auth or not check_auth(auth.username, auth.password):
        return Response(
            "Pristup odepren. Zadejte prihlasovaci udaje.",
            401,
            {"WWW-Authenticate": 'Basic realm="Sintera Radar"'},
        )


# ── Správa úloh (background scraping) ────────────────────────────────────────
# job = {status, zprava, celkem, kraj_index, kraj_total, nabidky, db_stats, chyba, started}
_jobs: dict = {}
_jobs_lock = threading.Lock()


def _new_job() -> str:
    jid = uuid.uuid4().hex[:12]
    with _jobs_lock:
        _jobs[jid] = {
            "status": "pending",
            "zprava": "Připravuji...",
            "celkem": 0,
            "kraj_index": 0,
            "kraj_total": 1,
            "nabidky": [],
            "db_stats": None,
            "chyba": None,
            "started": datetime.now().isoformat(timespec="seconds"),
        }
    return jid


def _update_job(jid: str, **kw):
    with _jobs_lock:
        if jid in _jobs:
            _jobs[jid].update(kw)


def _run_scraping(jid: str, params: dict):
    """Běží v background threadu. Průběžně aktualizuje _jobs[jid]."""
    try:
        _update_job(jid, status="running", zprava="Startuji...")

        vse_kraje = params.get("vse_kraje", False)

        def cb(zprava, celkem, kraj_index=1, kraj_total=1):
            _update_job(jid, zprava=zprava, celkem=celkem,
                        kraj_index=kraj_index, kraj_total=kraj_total)

        if vse_kraje:
            nabidky = scrape_vsechny_kraje(
                obory=params["obory"],
                uvazek=params["uvazek"],
                prima_zamestnavatele=params["prima"],
                home_office=params["ho"],
                on_progress=cb,
            )
        else:
            nabidky = scrape(
                kraj_slug=params["kraj_slug"],
                kraj_nazev=params["kraj_nazev"],
                hledat=params["hledat"],
                obory=params["obory"],
                uvazek=params["uvazek"],
                prima_zamestnavatele=params["prima"],
                home_office=params["ho"],
                max_stranky=params["stranky"],
                on_progress=lambda z, c: cb(z, c, 1, 1),
            )

        db_stats = None
        if params.get("do_db") and nabidky:
            _update_job(jid, zprava=f"Ukládám {len(nabidky)} nabídek do databáze...")
            db_stats = uloz_nabidky([n.__dict__ for n in nabidky])

        _update_job(jid,
                    status="done",
                    zprava=f"Hotovo — staženo {len(nabidky)} nabídek",
                    celkem=len(nabidky),
                    nabidky=[n.__dict__ for n in nabidky],
                    db_stats=db_stats)

    except Exception as e:
        _update_job(jid, status="error", chyba=str(e),
                    zprava=f"Chyba: {e}")


# ── Routy ─────────────────────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    d_stats = dashboard_stats()
    surges = detect_surge(threshold=4)
    recommendations = radar_doporuceni(per_region=5)
    o_stats = outreach_statistiky()
    return render_template("dashboard.html",
                           d_stats=d_stats, surges=surges,
                           recommendations=recommendations, o_stats=o_stats,
                           kraje=KRAJE)


@app.route("/pozice")
def pozice_page():
    filtr = request.args.get("filtr", "")
    kraj = request.args.get("kraj", "")
    sort = request.args.get("sort", "")
    rows = filtr_pozice(filtr=filtr, kraj=kraj, limit=500, sort=sort)

    titulky = {
        "aktualizovane": "Aktualizovane pozice",
        "starnouci": "Starnouci pozice (21+ dni)",
        "nove_tyden": "Nove pozice tento tyden",
        "aktivni": "Vsechny aktivni pozice",
        "opakovane": "Opakovana zadani",
        "problematicke": "Problematicke pozice (4+ scanu)",
        "nejstarsi": "Nejdele otevrene pozice",
    }
    popisky = {
        "aktualizovane": "Pozice oznacene jako Aktualizovano na jobs.cz - firma hledala, nenasla, a obnovila inzerat. Nejsilnejsi signal pro osloveni.",
        "starnouci": "Pozice otevrene dele nez 3 tydny. Firma pravdepodobne nema kapacitu obsadit pozici sama.",
        "nove_tyden": "Pozice pridane v poslednich 7 dnech.",
        "aktivni": "Vsechny aktualne aktivni pozice na jobs.cz.",
        "opakovane": "Firma zadala stejnou pozici opakovane - predchozi inzerat zanikl a znovuotevreli. Signal ze neumi obsadit.",
        "problematicke": "Pozice videne ve 4 a vice scanech - firma je dlouhodobe neobsazuje.",
        "nejstarsi": "Vsechny pozice serazene od nejdele otevrenych. Cim dele otevrena, tim vetsi pravdepodobnost ze firma potrebuje pomoc.",
    }

    return render_template("pozice.html",
                           rows=rows, filtr=filtr, kraj=kraj, sort=sort,
                           titulek=titulky.get(filtr, "Pozice"),
                           popisek=popisky.get(filtr, ""),
                           kraje=KRAJE)


@app.route("/firmy")
def firmy_page():
    sort = request.args.get("sort", "pozic")
    page = int(request.args.get("page", 1))
    kraj = request.args.get("kraj", "")
    data = firmy_prehled(sort=sort, page=page, per_page=50, kraj=kraj)
    return render_template("firmy.html",
                           data=data, sort=sort, page=page, kraj=kraj,
                           kraje=KRAJE)


@app.route("/scraper")
def scraper_page():
    stats = statistiky()
    return render_template("index.html",
                           kraje=KRAJE, obory=OBORY,
                           uvazky=UVAZKY, home_office_opts=HOME_OFFICE,
                           stats=stats, nabidky=None, job_id=None)


@app.route("/hledat", methods=["POST"])
def hledat():
    kraj_kod   = request.form.get("kraj", "")
    hledat_kw  = request.form.get("hledat", "").strip()
    stranky    = int(request.form.get("stranky", 999))
    obory_sel  = request.form.getlist("obory")
    uvazek_sel = request.form.getlist("uvazek")
    ho_sel     = request.form.getlist("home_office")
    prima      = "prima" in request.form
    do_db      = "do_db" in request.form

    vse_kraje = (kraj_kod == "vse")
    # "Celá ČR" (prázdný kraj) → automaticky přepnout na per-region mód,
    # protože jobs.cz /prace/ bez regionu vrací JS-only stránku (0 výsledků)
    # a navíc jeden dotaz je limitovaný na 1 350 výsledků.
    if not kraj_kod:
        vse_kraje = True
    if not vse_kraje and kraj_kod:
        kraj_nazev, kraj_slug = KRAJE.get(kraj_kod, ("Celá ČR", ""))
    else:
        kraj_nazev, kraj_slug = ("Celá ČR", "")

    params = dict(
        vse_kraje=vse_kraje,
        kraj_slug=kraj_slug, kraj_nazev=kraj_nazev,
        hledat=hledat_kw, obory=obory_sel,
        uvazek=uvazek_sel, prima=prima, ho=ho_sel,
        stranky=stranky, do_db=do_db,
    )

    jid = _new_job()
    t = threading.Thread(target=_run_scraping, args=(jid, params), daemon=True)
    t.start()

    stats = statistiky()
    return render_template("index.html",
                           kraje=KRAJE, obory=OBORY,
                           uvazky=UVAZKY, home_office_opts=HOME_OFFICE,
                           stats=stats, nabidky=None,
                           job_id=jid,
                           kraj_kod=kraj_kod, hledat=hledat_kw,
                           stranky=stranky,
                           obory_sel=obory_sel, uvazek_sel=uvazek_sel,
                           ho_sel=ho_sel, prima=prima,
                           kraj_nazev=kraj_nazev)


@app.route("/api/progress/<jid>")
def api_progress(jid: str):
    with _jobs_lock:
        job = _jobs.get(jid)
    if not job:
        return jsonify({"status": "not_found"}), 404
    return jsonify({
        "status":      job["status"],
        "zprava":      job["zprava"],
        "celkem":      job["celkem"],
        "kraj_index":  job["kraj_index"],
        "kraj_total":  job["kraj_total"],
        "db_stats":    job["db_stats"],
        "chyba":       job["chyba"],
    })


@app.route("/api/results/<jid>")
def api_results(jid: str):
    with _jobs_lock:
        job = _jobs.get(jid)
    if not job or job["status"] != "done":
        return jsonify({"status": job["status"] if job else "not_found"}), 202
    return jsonify({
        "status":   "done",
        "nabidky":  job["nabidky"],
        "db_stats": job["db_stats"],
    })


@app.route("/api/save/<jid>", methods=["POST"])
def api_save(jid: str):
    """Uloží výsledky hotového jobu do databáze (post-hoc)."""
    with _jobs_lock:
        job = _jobs.get(jid)
    if not job:
        return jsonify({"error": "Job nenalezen"}), 404
    if job["status"] != "done":
        return jsonify({"error": "Job ještě nedoběhl"}), 400
    if job.get("db_stats"):
        return jsonify({"error": "Již uloženo", "db_stats": job["db_stats"]}), 200
    nabidky = job.get("nabidky", [])
    if not nabidky:
        return jsonify({"error": "Žádné nabídky k uložení"}), 400
    db_stats = uloz_nabidky(nabidky)
    with _jobs_lock:
        _jobs[jid]["db_stats"] = db_stats
    return jsonify({"ok": True, "db_stats": db_stats})


@app.route("/databaze")
def databaze():
    kraj    = request.args.get("kraj", "")
    min_sc  = int(request.args.get("min_scanu", 0))
    offset  = int(request.args.get("offset", 0))
    aktivni = request.args.get("aktivni", "") == "1"
    rows  = nacti_nabidky(kraj=kraj, aktivni_pouze=aktivni,
                          min_scanu=min_sc, limit=100, offset=offset)
    stats = statistiky()
    return render_template("databaze.html",
                           kraje=KRAJE, rows=rows, stats=stats,
                           kraj=kraj, min_scanu=min_sc,
                           offset=offset, aktivni=aktivni)


@app.route("/export-db")
def export_db():
    path = exportuj_csv("data/export_full.csv")
    with open(path, encoding="utf-8-sig") as f:
        content = f.read()
    return Response(
        content.encode("utf-8-sig"),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=jobs_databaze.csv"},
    )


@app.route("/export", methods=["POST"])
def export():
    jid = request.form.get("job_id", "")
    with _jobs_lock:
        job = _jobs.get(jid)
    if not job or not job.get("nabidky"):
        return "Výsledky nenalezeny", 404

    nabidky = job["nabidky"]
    buf = io.StringIO()
    if nabidky:
        writer = csv.DictWriter(buf, fieldnames=list(nabidky[0].keys()))
        writer.writeheader()
        writer.writerows(nabidky)
    return Response(
        buf.getvalue().encode("utf-8-sig"),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=jobs_export.csv"},
    )


# ── Decision Makers ──────────────────────────────────────────────────────────

@app.route("/decision-makers")
def decision_makers_page():
    kraj = request.args.get("kraj", "")
    dm_stats = dm_statistiky()
    dms = nacti_decision_makers(kraj=kraj, limit=200)
    return render_template("decision_makers.html",
                           kraje=KRAJE, dm_stats=dm_stats, dms=dms,
                           selected_kraj=kraj)


@app.route("/upload-dm", methods=["POST"])
def upload_dm():
    kraj = request.form.get("kraj", "")
    if not kraj:
        return "Vyberte kraj", 400

    files = request.files.getlist("csv_files")
    if not files or not files[0].filename:
        return "Vyberte alespoň jeden CSV soubor", 400

    total_imported = 0
    total_skipped = 0

    for f in files:
        content = f.read().decode("utf-8-sig", errors="replace")
        result = import_linkedin_csv(content, kraj)
        total_imported += result["imported"]
        total_skipped += result["skipped"]

    return redirect(url_for("decision_makers_page", kraj=kraj,
                            msg="ok", imported=total_imported, skipped=total_skipped))


@app.route("/add-dm", methods=["POST"])
def add_dm():
    data = {
        "first_name": request.form.get("first_name", ""),
        "last_name": request.form.get("last_name", ""),
        "current_title": request.form.get("current_title", ""),
        "current_company": request.form.get("current_company", ""),
        "profile_url": request.form.get("profile_url", ""),
        "kraj": request.form.get("kraj", ""),
    }
    add_decision_maker(data)
    return redirect(url_for("decision_makers_page", kraj=data["kraj"], msg="added"))


# ── Radar ────────────────────────────────────────────────────────────────────

@app.route("/radar")
def radar_page():
    result = radar_matches(skip_fuzzy=True)
    dm_stats = dm_statistiky()
    surges = detect_surge(threshold=4)
    recommendations = radar_doporuceni(per_region=3, skip_fuzzy=True)
    o_stats = outreach_statistiky()
    return render_template("radar.html",
                           kraje=KRAJE, result=result, dm_stats=dm_stats,
                           surges=surges, recommendations=recommendations,
                           o_stats=o_stats, OUTREACH_STATUSES=OUTREACH_STATUSES)


@app.route("/api/outreach-brief/<firma_norm>")
def api_outreach_brief(firma_norm):
    brief = outreach_brief(firma_norm)
    if not brief:
        return jsonify({"error": "Firma nenalezena"}), 404
    # Format as plain text for copy-paste into ChatGPT
    lines = []
    lines.append("Company: {}".format(brief["firma"]))
    lines.append("Region: {}".format(brief["kraje"]))
    lines.append("Signal: {}".format(brief["signal"]))
    lines.append("Open positions ({} total, longest active {} weeks):".format(
        brief["pocet_pozic"], brief["max_scanu"]))
    for p in brief["pozice"]:
        lines.append("  - {}".format(p))
    lines.append("")
    lines.append("Decision maker(s):")
    for dm in brief["decision_makers"]:
        lines.append("  - {}".format(dm))
    lines.append("")
    lines.append("---")
    lines.append("Write a personalized, short LinkedIn connection request or InMail "
                 "in Czech language. Be professional, reference their hiring needs "
                 "without being pushy. We are Sintera, a recruitment agency.")
    return jsonify({"brief": "\n".join(lines), "data": brief})


@app.route("/firma/<path:firma_norm>")
def firma_detail_page(firma_norm):
    detail = firma_detail(firma_norm)
    if not detail:
        return "Firma nenalezena", 404
    o_stats = outreach_statistiky()
    return render_template("firma_detail.html",
                           detail=detail, kraje=KRAJE, o_stats=o_stats,
                           OUTREACH_STATUSES=OUTREACH_STATUSES)


@app.route("/add-alias", methods=["POST"])
def add_alias():
    firma_jobs = request.form.get("firma_jobs", "")
    firma_linkedin = request.form.get("firma_linkedin", "")
    if firma_jobs and firma_linkedin:
        add_company_alias(firma_jobs, firma_linkedin)
    return redirect(url_for("radar_page", msg="alias_added"))


# ── Outreach CRM ────────────────────────────────────────────────────────────

@app.route("/api/mark-contacted", methods=["POST"])
def api_mark_contacted():
    data = request.get_json()
    firma = data.get("firma", "")
    firma_norm = data.get("firma_norm", "")
    kontakt = data.get("kontakt", "")
    kontakt_url = data.get("kontakt_url", "")
    kanal = data.get("kanal", "LinkedIn DM")
    kraj = data.get("kraj", "")
    oid = add_outreach(firma, firma_norm, kontakt, kontakt_url, kanal, kraj=kraj)
    return jsonify({"ok": True, "id": oid})


@app.route("/api/update-outreach", methods=["POST"])
def api_update_outreach():
    data = request.get_json()
    oid = data.get("id")
    status = data.get("status", "")
    poznamka = data.get("poznamka", "")
    if oid and status:
        update_outreach_status(int(oid), status, poznamka)
    return jsonify({"ok": True})


@app.route("/outreach")
def outreach_page():
    status_filter = request.args.get("status", "")
    records = nacti_outreach(status=status_filter, limit=200)
    o_stats = outreach_statistiky()
    return render_template("outreach.html",
                           records=records, o_stats=o_stats,
                           OUTREACH_STATUSES=OUTREACH_STATUSES,
                           status_filter=status_filter, kraje=KRAJE)


# ── Batch messages ──────────────────────────────────────────────────────────

@app.route("/batch")
def batch_page():
    per_region = int(request.args.get("per_region", 3))
    recommendations = radar_doporuceni(per_region=per_region)
    messages = generate_batch_messages(recommendations)
    return render_template("batch.html",
                           messages=messages, per_region=per_region, kraje=KRAJE)


# ── MISSILE DM Generator ───────────────────────────────────────────────────

@app.route("/missile")
def missile_page():
    kraj = request.args.get("kraj", "")
    limit = int(request.args.get("limit", 50))
    dms = generate_missile_dms(kraj=kraj, limit=limit)
    return render_template("missile.html",
                           dms=dms, kraj=kraj, limit=limit, kraje=KRAJE)


# ── Salary Benchmark ───────────────────────────────────────────────────────

@app.route("/benchmark")
def benchmark_page():
    kraj = request.args.get("kraj", "")
    keyword = request.args.get("keyword", "").strip()
    data = salary_benchmark(kraj=kraj, keyword=keyword)
    return render_template("benchmark.html",
                           data=data, kraj=kraj, keyword=keyword, kraje=KRAJE)


# ── Analytics Export (CSV for Looker Studio) ────────────────────────────────

@app.route("/analytics")
def analytics_page():
    return render_template("analytics.html", kraje=KRAJE)


@app.route("/api/analytics.csv")
def api_analytics_csv():
    rows = analytics_export_data()
    if not rows:
        return Response("No data", mimetype="text/plain")
    buf = io.StringIO()
    fieldnames = [
        "firma", "pozice", "kraj", "obor", "plat_text", "plat_od", "plat_do",
        "plat_stred", "ma_plat", "url", "pocet_scanu", "dni_aktivni",
        "datum_prvni", "datum_posledni", "publikovano",
        "je_aktualizovano", "je_opakovane", "je_problematicke", "je_starnouci",
    ]
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return Response(
        buf.getvalue().encode("utf-8-sig"),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=sintera_analytics.csv"},
    )


if __name__ == "__main__":
    import os
    host = os.environ.get("FLASK_HOST", "127.0.0.1")
    debug = os.environ.get("FLASK_ENV") != "production"
    app.run(host=host, debug=debug, port=5001, threaded=True)
