#!/usr/bin/env python3
"""
Jobs.cz scraper — hledání pracovních pozic podle krajů ČR.
Podporuje stažení VŠECH stránek výsledků a filtrování podle oborů.

URL formát jobs.cz (2026):
  https://www.jobs.cz/prace/{kraj}/{obor}/{uvazek}/?employer=direct&page=N

CLI použití:
  python scraper.py [--kraj <zkratka>] [--obory <o1,o2>] [--filtry ...] [--db] [--csv]
"""

import argparse
import csv
import re
import sys
import time
from dataclasses import dataclass, field, fields
from datetime import date, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ── Kraje ─────────────────────────────────────────────────────────────────────

KRAJE: dict[str, tuple[str, str]] = {
    "pha": ("Praha",                   "praha"),
    "stc": ("Středočeský kraj",        "stredocesky-kraj"),
    "jhc": ("Jihočeský kraj",          "jihocesky-kraj"),
    "plk": ("Plzeňský kraj",           "plzensky-kraj"),
    "kvk": ("Karlovarský kraj",        "karlovarsky-kraj"),
    "ulk": ("Ústecký kraj",            "ustecky-kraj"),
    "lbk": ("Liberecký kraj",          "liberecky-kraj"),
    "hkk": ("Královéhradecký kraj",    "kralovehradecky-kraj"),
    "pak": ("Pardubický kraj",         "pardubicky-kraj"),
    "vys": ("Kraj Vysočina",           "vysocina-kraj"),
    "jhm": ("Jihomoravský kraj",       "jihomoravsky-kraj"),
    "olk": ("Olomoucký kraj",          "olomoucky-kraj"),
    "zlk": ("Zlínský kraj",            "zlinsky-kraj"),
    "msk": ("Moravskoslezský kraj",    "moravskoslezsky-kraj"),
}

# ── Filtry oborů — slug pro URL path ─────────────────────────────────────────
# Jobs.cz 2026: obor je součást URL path: /prace/{kraj}/{obor-slug}/
# Pouze JEDEN obor v URL; pro více oborů se scrapuje postupně.

OBORY: dict[str, str] = {
    "strojirenstvi":    "strojirenstvi",
    "technika":         "technika-a-vyvoj",
    "elektrotechnika":  "elektrotechnika-a-energetika",
    "vyroba":           "vyroba-a-prumysl",
    "veda":             "veda-a-vyzkum",
    "chemie":           "chemicky-prumysl",
    "doprava":          "doprava-logistika-a-zasobovani",
    "farmacie":         "farmacie",
    "ekonomika":        "ekonomika-a-podnikove-finance",
    "kvalita":          "kvalita-a-kontrola-jakosti",
    "remesla":          "remeslne-a-manualni-prace",
    "nakup":            "nakup",
    "hr":               "personalistika-a-hr",
    "management":       "vrcholovy-management",
    "telekomunikace":   "telekomunikace",
    "zakaznicky":       "zakaznicky-servis",
    "it_konzultace":    "is-it-konzultace-analyzy-a-projektove-rizeni",
    "it_sprava":        "is-it-sprava-systemu-a-hw",
    "it_vyvoj":         "is-it-vyvoj-aplikaci-a-systemu",
    "finance":          "bankovnictvi-a-financni-sluzby",
    "marketing":        "marketing",
    "obchod":           "prodej-a-obchod",
    "administrativa":   "administrativa",
    "zdravotnictvi":    "zdravotnictvi-a-socialni-pece",
    "stavebnictvi":     "stavebnictvi-a-reality",
}

# Typ úvazku — slug pro URL path: /prace/{kraj}/{obor}/{uvazek-slug}/
UVAZKY: dict[str, str] = {
    "full":   "plny-uvazek",
    "part":   "zkraceny-uvazek",
}

# Home office — query parametr
HOME_OFFICE: dict[str, str] = {
    "moznost":    "occasionally",
    "prevazne":   "mostly",
    "flexibilni": "flexible",
}

# ── Mapování město → kraj (pro určení kraje z místa v inzerátu) ──────────────

_MESTO_KRAJ: dict[str, str] = {}

def _init_mesto_kraj():
    """Naplní mapování město→kraj pro ~200 největších měst ČR."""
    _data = {
        "Praha": [
            "Praha",
        ],
        "Středočeský kraj": [
            "Kladno", "Mladá Boleslav", "Příbram", "Kolín", "Kutná Hora", "Benešov",
            "Beroun", "Mělník", "Nymburk", "Rakovník", "Poděbrady", "Brandýs nad Labem",
            "Říčany", "Černošice", "Nehvizdy", "Lysá nad Labem", "Kralupy nad Vltavou",
            "Slaný", "Neratovice", "Čáslav", "Vlašim", "Sedlčany", "Dobříš", "Hořovice",
            "Mnichovo Hradiště", "Čelákovice", "Milovice", "Roztoky", "Úvaly", "Hostivice",
            "Jesenice", "Rudná", "Odolena Voda", "Stochov", "Libčice nad Vltavou",
        ],
        "Jihočeský kraj": [
            "České Budějovice", "Tábor", "Písek", "Strakonice", "Prachatice",
            "Český Krumlov", "Jindřichův Hradec", "Třeboň", "Soběslav", "Blatná",
            "Vimperk", "Milevsko", "Kaplice", "Dačice", "Týn nad Vltavou", "Protivín",
            "Vodňany", "Sezimovo Ústí",
        ],
        "Plzeňský kraj": [
            "Plzeň", "Klatovy", "Domažlice", "Rokycany", "Tachov", "Sušice",
            "Stříbro", "Nýřany", "Přeštice", "Vejprnice", "Dobřany", "Holýšov",
            "Nepomuk", "Blovice", "Horažďovice", "Kralovice", "Nová Bystřice",
        ],
        "Karlovarský kraj": [
            "Karlovy Vary", "Cheb", "Sokolov", "Mariánské Lázně", "Aš", "Ostrov",
            "Chodov", "Františkovy Lázně", "Kraslice", "Habartov", "Nejdek",
        ],
        "Ústecký kraj": [
            "Ústí nad Labem", "Teplice", "Most", "Chomutov", "Děčín", "Litoměřice",
            "Louny", "Žatec", "Bílina", "Kadaň", "Jirkov", "Klášterec nad Ohří",
            "Roudnice nad Labem", "Lovosice", "Rumburk", "Varnsdorf", "Duchcov",
            "Krupka", "Litvínov",
        ],
        "Liberecký kraj": [
            "Liberec", "Jablonec nad Nisou", "Česká Lípa", "Turnov", "Semily",
            "Tanvald", "Jilemnice", "Nový Bor", "Frýdlant", "Železný Brod", "Doksy",
            "Hrádek nad Nisou", "Mimoň",
        ],
        "Královéhradecký kraj": [
            "Hradec Králové", "Trutnov", "Náchod", "Jičín", "Rychnov nad Kněžnou",
            "Dvůr Králové nad Labem", "Vrchlabí", "Broumov", "Hořice", "Nová Paka",
            "Jaroměř", "Kostelec nad Orlicí", "Nové Město nad Metují", "Dobruška",
            "Chlumec nad Cidlinou", "Červený Kostelec", "Úpice",
        ],
        "Pardubický kraj": [
            "Pardubice", "Chrudim", "Svitavy", "Ústí nad Orlicí", "Vysoké Mýto",
            "Litomyšl", "Moravská Třebová", "Polička", "Žamberk", "Lanškroun",
            "Česká Třebová", "Choceň", "Přelouč", "Holice", "Lázně Bohdaneč",
            "Letohrad", "Skuteč",
        ],
        "Kraj Vysočina": [
            "Jihlava", "Havlíčkův Brod", "Třebíč", "Žďár nad Sázavou", "Pelhřimov",
            "Humpolec", "Velké Meziříčí", "Nové Město na Moravě", "Chotěboř",
            "Světlá nad Sázavou", "Bystřice nad Pernštejnem", "Moravské Budějovice",
            "Telč", "Náměšť nad Oslavou", "Pacov", "Velká Bíteš",
        ],
        "Jihomoravský kraj": [
            "Brno", "Znojmo", "Břeclav", "Hodonín", "Blansko", "Vyškov", "Boskovice",
            "Mikulov", "Hustopeče", "Kyjov", "Veselí nad Moravou", "Slavkov u Brna",
            "Kuřim", "Pohořelice", "Tišnov", "Ivančice", "Rosice", "Bučovice",
            "Šlapanice", "Modřice", "Valtice", "Bošovice", "Rajhrad", "Židlochovice",
            "Oslavany", "Letovice",
        ],
        "Olomoucký kraj": [
            "Olomouc", "Přerov", "Prostějov", "Šumperk", "Jeseník", "Zábřeh",
            "Lipník nad Bečvou", "Konice", "Litovel", "Šternberk", "Uničov",
            "Hranice", "Mohelnice", "Loštice", "Tovačov", "Lutín", "Velká Bystřice",
            "Hlubočky", "Dolany",
        ],
        "Zlínský kraj": [
            "Zlín", "Vsetín", "Kroměříž", "Uherské Hradiště", "Vizovice",
            "Valašské Meziříčí", "Otrokovice", "Rožnov pod Radhoštěm", "Luhačovice",
            "Holešov", "Bystřice pod Hostýnem", "Uherský Brod", "Napajedla",
            "Slavičín", "Valašské Klobouky", "Hulín",
        ],
        "Moravskoslezský kraj": [
            "Ostrava", "Opava", "Frýdek-Místek", "Karviná", "Havířov", "Nový Jičín",
            "Třinec", "Orlová", "Český Těšín", "Kopřivnice", "Bruntál", "Bohumín",
            "Krnov", "Hlučín", "Studénka", "Frenštát pod Radhoštěm", "Příbor",
            "Bílovec", "Nošovice", "Fulnek", "Frýdlant nad Ostravicí", "Jablunkov",
            "Petřvald", "Rychvald", "Šenov",
        ],
    }
    for kraj, mesta in _data.items():
        for mesto in mesta:
            _MESTO_KRAJ[mesto.lower()] = kraj

_init_mesto_kraj()


def _urcit_kraj_z_mista(misto: str) -> str:
    """Pokusí se z textu místa (např. 'Brno – Slatina') určit kraj."""
    if not misto:
        return ""
    # Ořež "  + N další lokalita"
    text = re.sub(r"\s*\+\s*\d+\s+další.*$", "", misto)
    # Ořež "  – čtvrť"
    text = re.split(r"\s+[–—-]\s+", text)[0].strip()
    return _MESTO_KRAJ.get(text.lower(), "")


BASE_URL = "https://www.jobs.cz/prace"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "cs,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Datová třída ───────────────────────────────────────────────────────────────

@dataclass
class Nabidka:
    job_id:    str
    pozice:    str
    firma:     str
    misto:     str
    kraj:      str
    kraj_slug: str
    plat:      str
    url:       str
    obor:           str = ""   # slug oboru (strojirenstvi, technika-a-vyvoj…)
    publikovano:    str = ""   # originální text ("Aktualizováno včera", "11. března")
    datum_vydani:   str = ""   # vypočtené ISO datum (2026-03-11)
    datum_scrapovani: str = "" # datum scrapování (2026-04-02)


# ── URL builder ───────────────────────────────────────────────────────────────

def build_url(
    kraj_slug: str = "",
    obor_slug: str = "",
    uvazek_slug: str = "",
    hledat: str = "",
    prima_zamestnavatele: bool = False,
    home_office: list = None,
    page: int = 1,
) -> str:
    """
    Sestaví URL ve formátu jobs.cz 2026:
      /prace/{kraj}/{obor}/{uvazek}/?employer=direct&page=N&q[]=...
    """
    home_office = home_office or []

    # Cesta: /prace/ + volitelné segmenty
    path_parts = ["prace"]
    if kraj_slug:
        path_parts.append(kraj_slug)
    if obor_slug:
        path_parts.append(obor_slug)
    if uvazek_slug:
        path_parts.append(uvazek_slug)

    path = "/".join(path_parts) + "/"
    base = f"https://www.jobs.cz/{path}"

    # Query parametry
    params = []
    if prima_zamestnavatele:
        params.append("employer=direct")
    if hledat:
        params.append(f"q[]={requests.utils.quote(hledat)}")
    for ho in home_office:
        slug = HOME_OFFICE.get(ho, ho)
        params.append(f"home_office={slug}")
    if page > 1:
        params.append(f"page={page}")

    return f"{base}?{'&'.join(params)}" if params else base


# ── Parsování stránky ─────────────────────────────────────────────────────────

def _extract_job_id(url: str) -> str:
    """https://www.jobs.cz/rpd/2001148027/?... -> '2001148027'"""
    m = re.search(r"/rpd/(\d+)/", url)
    return m.group(1) if m else ""


# ── Parsování českých datumů ──────────────────────────────────────────────────

_MESICE = {
    "ledna": 1, "února": 2, "března": 3, "dubna": 4,
    "května": 5, "června": 6, "července": 7, "srpna": 8,
    "září": 9, "října": 10, "listopadu": 11, "prosince": 12,
}


def _parse_datum_vydani(text: str) -> str:
    """
    Převede český text o datu na ISO datum (YYYY-MM-DD).
    Příklady:
      '11. března'              → '2026-03-11'
      'Aktualizováno včera'     → včerejší datum
      'Příležitost dne'         → dnešní datum
      'Končí za 23 hodin'      → dnešní datum
      'Končí zítra'             → dnešní datum (inzerát stále aktivní)
      'Aktualizováno před 14 minutami' → dnešní datum
      'Přidáno před 3 dny'     → 3 dny zpět
    """
    if not text:
        return ""

    dnes = date.today()

    # Absolutní datum: "11. března", "2. dubna"
    m = re.match(r"(\d{1,2})\.\s*(\w+)", text.strip())
    if m:
        den = int(m.group(1))
        mesic_text = m.group(2).lower()
        mesic = _MESICE.get(mesic_text)
        if mesic:
            rok = dnes.year
            try:
                d = date(rok, mesic, den)
                # Pokud je datum v budoucnosti o víc než měsíc, asi jde o minulý rok
                if d > dnes + timedelta(days=31):
                    d = date(rok - 1, mesic, den)
                return d.isoformat()
            except ValueError:
                pass

    t = text.lower()

    # Příležitost dne / dnes / právě přidáno
    if any(k in t for k in ("příležitost dne", "dnes", "právě", "minutami", "hodinou", "hodinami", "hodin")):
        return dnes.isoformat()

    # Včera
    if "včera" in t:
        return (dnes - timedelta(days=1)).isoformat()

    # Před X dny
    m = re.search(r"před\s+(\d+)\s*dn", t)
    if m:
        return (dnes - timedelta(days=int(m.group(1)))).isoformat()

    # Před X týdny
    m = re.search(r"před\s+(\d+)\s*týdn", t)
    if m:
        return (dnes - timedelta(weeks=int(m.group(1)))).isoformat()

    # Končí za / zítra — inzerát je stále aktivní, bereme dnešek
    if any(k in t for k in ("končí", "zítra")):
        return dnes.isoformat()

    return ""


def _extract_plat(article: BeautifulSoup) -> str:
    """Najde text s platem — prioritne hleda text s Kc bez detskych tagu."""
    for el in article.find_all(True):
        if el.find(True):          # ma deti -> preskocit
            continue
        text = el.get_text(strip=True)
        if ("Kc" in text or "Kč" in text or "EUR" in text) and len(text) < 70:
            return text
    return ""


def parse_article(article: BeautifulSoup, kraj: str, kraj_slug: str) -> Optional[Nabidka]:
    heading_link = article.select_one("header h2 a, header h3 a")
    if not heading_link:
        return None

    pozice = heading_link.get_text(strip=True)
    url    = heading_link.get("href", "")
    if url and not url.startswith("http"):
        url = "https://www.jobs.cz" + url

    job_id = _extract_job_id(url)
    if not job_id:
        return None

    footer = article.select_one("footer, [role='contentinfo']")
    items  = footer.select("li") if footer else []
    firma  = items[0].get_text(strip=True) if len(items) >= 1 else ""
    misto  = items[1].get_text(strip=True) if len(items) >= 2 else ""

    plat = _extract_plat(article)

    # Datum zverejneni / aktualizace — jobs.cz 2026 pouziva
    # <div class="SearchResultCard__status">Aktualizováno včera</div>
    pub_el = article.select_one(".SearchResultCard__status")
    publikovano = pub_el.get_text(strip=True) if pub_el else ""
    datum_vydani = _parse_datum_vydani(publikovano)

    # Pokud scrapujeme "Celá ČR", zkusíme určit kraj z města
    skutecny_kraj = kraj
    if not kraj or kraj == "Celá ČR":
        skutecny_kraj = _urcit_kraj_z_mista(misto) or kraj

    return Nabidka(
        job_id=job_id,
        pozice=pozice,
        firma=firma,
        misto=misto,
        kraj=skutecny_kraj,
        kraj_slug=kraj_slug,
        plat=plat,
        url=url,
        publikovano=publikovano,
        datum_vydani=datum_vydani,
        datum_scrapovani=date.today().isoformat(),
    )


def scrape_page(
    url: str,
    session: requests.Session,
    kraj: str,
    kraj_slug: str,
) -> tuple[list[Nabidka], bool]:
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  ⚠ Chyba {url}: {e}", file=sys.stderr)
        return [], False

    soup = BeautifulSoup(resp.text, "html.parser")

    # Pouze reálné nabídky (article.SearchResultCard), ne reklamy/modaly
    nabidky = []
    for art in soup.select("article.SearchResultCard"):
        n = parse_article(art, kraj, kraj_slug)
        if n:
            nabidky.append(n)

    # Další strana: jobs.cz 2026 používá tlačítko s CSS třídou
    # Pagination__button--next (SVG šipka + hidden <span>Následující</span>).
    # Žádný rel="next" ani aria-label.
    has_next = bool(
        soup.select_one(".Pagination__button--next")
    )

    return nabidky, has_next


# ── Hlavní scraping funkce ────────────────────────────────────────────────────

def scrape(
    kraj_slug: str = "",
    kraj_nazev: str = "Celá ČR",
    hledat: str = "",
    obory=None,
    uvazek=None,
    prima_zamestnavatele: bool = False,
    home_office=None,
    max_stranky: int = 999,
    verbose: bool = True,
    on_progress=None,   # callback(zprava: str, celkem: int)
) -> list:
    """
    Projde vsechny dostupne strany (do max_stranky) a vrati seznam nabidek.
    Jobs.cz podporuje max 1 obor v URL — pri vice oborech se scrapuje postupne
    a vysledky se deduplikuji.
    on_progress(zprava, celkem_dosud) se vola po kazde strance.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    obory       = obory or []
    uvazek      = uvazek or []
    home_office = home_office or []

    # Převod úvazku na path slug — bereme první (jobs.cz podporuje jen 1 v path)
    uvazek_slug = ""
    if uvazek:
        uvazek_slug = UVAZKY.get(uvazek[0], "")

    # Jobs.cz vrací max 1 350 výsledků (45 stran × 30) na jeden dotaz.
    # Aby se nic neztratilo, VŽDY scrapujeme po jednotlivých oborech —
    # každý obor má vlastní 1 350-limit, takže celkově pokryjeme vše.
    # Deduplikace přes seen_ids zajistí, že se nic nezdvojí.
    obor_slugs = []
    if obory:
        for o in obory:
            slug = OBORY.get(o, o)
            obor_slugs.append(slug)
    else:
        # Žádné obory nevybrány → projdeme všechny definované obory
        obor_slugs = list(OBORY.values())

    if not obor_slugs:
        obor_slugs = [""]   # fallback — bez filtru

    vsechny = []
    seen_ids: set[str] = set()

    for obor_idx, obor_slug in enumerate(obor_slugs):
        page = 1
        obor_label = obor_slug or "vše"

        while page <= max_stranky:
            url = build_url(
                kraj_slug=kraj_slug,
                obor_slug=obor_slug,
                uvazek_slug=uvazek_slug,
                hledat=hledat,
                prima_zamestnavatele=prima_zamestnavatele,
                home_office=home_office,
                page=page,
            )
            if verbose:
                print(f"  [{kraj_nazev}|{obor_label}] strana {page}: {url}")

            nabidky, has_next = scrape_page(url, session, kraj_nazev, kraj_slug)

            # Nastav obor a deduplikace
            new_nabidky = []
            for n in nabidky:
                n.obor = obor_slug
                if n.job_id not in seen_ids:
                    seen_ids.add(n.job_id)
                    new_nabidky.append(n)

            # Pojistka: pokud stránka nevrátila žádné nové nabídky, stop
            if nabidky and not new_nabidky:
                if verbose:
                    print(f"  → Strana {page} obsahuje jen duplikáty, ukončuji obor.")
                break

            vsechny.extend(new_nabidky)

            if verbose:
                print(f"  → {len(new_nabidky)} nových  (celkem {len(vsechny)})")

            if on_progress:
                on_progress(
                    f"{kraj_nazev} — {obor_label} str. {page} ({len(new_nabidky)} nových)",
                    len(vsechny),
                )

            if not nabidky or not has_next:
                break

            page += 1
            time.sleep(1.2)

        # Pauza mezi obory
        if obor_idx < len(obor_slugs) - 1:
            time.sleep(1.0)

    return vsechny


def scrape_vsechny_kraje(
    obory=None,
    uvazek=None,
    prima_zamestnavatele: bool = False,
    home_office=None,
    verbose: bool = True,
    on_progress=None,   # callback(zprava: str, celkem: int, kraj_index: int, kraj_total: int)
) -> list:
    """Projde vsech 14 kraju a vrati vsechny nabidky."""
    vsechny = []
    kraje_list = list(KRAJE.items())
    total_kraje = len(kraje_list)

    for i, (kod, (nazev, slug)) in enumerate(kraje_list, 1):
        if verbose:
            print(f"\n{'─'*60}\n  Kraj: {nazev} ({i}/{total_kraje})\n{'─'*60}")

        if on_progress:
            on_progress(
                f"Spouštím kraj {i}/{total_kraje}: {nazev}",
                len(vsechny), i, total_kraje,
            )

        def _cb(zprava, celkem, _i=i, _total=total_kraje):
            if on_progress:
                on_progress(zprava, celkem, _i, _total)

        nabidky = scrape(
            kraj_slug=slug, kraj_nazev=nazev,
            obory=obory, uvazek=uvazek,
            prima_zamestnavatele=prima_zamestnavatele,
            home_office=home_office,
            verbose=verbose,
            on_progress=_cb,
        )
        vsechny.extend(nabidky)
        time.sleep(2.0)

    return vsechny


# ── CLI výstup ────────────────────────────────────────────────────────────────

def tiskni_tabulku(nabidky: list) -> None:
    if not nabidky:
        print("Žádné nabídky nenalezeny.")
        return

    w_poz = min(50, max(len(n.pozice) for n in nabidky) + 2)
    w_fir = min(30, max(len(n.firma)  for n in nabidky) + 2)
    w_mis = min(25, max(len(n.misto)  for n in nabidky) + 2)
    w_pla = min(22, max((len(n.plat)  for n in nabidky), default=4) + 2)

    sep = f"+{'-'*w_poz}+{'-'*w_fir}+{'-'*w_mis}+{'-'*w_pla}+"
    fmt = f"|{{:<{w_poz}}}|{{:<{w_fir}}}|{{:<{w_mis}}}|{{:<{w_pla}}}|"

    print(sep)
    print(fmt.format(" Pozice", " Firma", " Místo", " Plat"))
    print(sep)
    for n in nabidky:
        print(fmt.format(
            f" {n.pozice[:w_poz-2]}",
            f" {n.firma[:w_fir-2]}",
            f" {n.misto[:w_mis-2]}",
            f" {n.plat[:w_pla-2]}",
        ))
    print(sep)
    print(f"\nCelkem: {len(nabidky)} nabídek\n")


def uloz_csv(nabidky: list, soubor: str) -> None:
    with open(soubor, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[fld.name for fld in fields(Nabidka)])
        writer.writeheader()
        for n in nabidky:
            writer.writerow(n.__dict__)
    print(f"Uloženo do: {soubor}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Jobs.cz scraper — všechny stránky, filtry oborů, databáze"
    )
    parser.add_argument("--kraj",    metavar="ZKRATKA",
                        help="Zkratka kraje (pha, jhm…) nebo 'vse' pro všechny")
    parser.add_argument("--hledat",  metavar="TEXT", default="",
                        help="Klíčové slovo")
    parser.add_argument("--obory",   metavar="SEZNAM", default="",
                        help="Čárkou oddělené obory: strojirenstvi,technika,elektrotechnika…")
    parser.add_argument("--uvazek",  metavar="SEZNAM", default="",
                        help="full,part")
    parser.add_argument("--prima",   action="store_true",
                        help="Pouze přímí zaměstnavatelé (bez agentur)")
    parser.add_argument("--ho",      metavar="SEZNAM", default="",
                        help="Home office: moznost,prevazne,flexibilni")
    parser.add_argument("--stranky", metavar="N", type=int, default=999,
                        help="Max počet stran (default: vše)")
    parser.add_argument("--db",      action="store_true",
                        help="Uloží výsledky do SQLite databáze")
    parser.add_argument("--csv",     metavar="SOUBOR", nargs="?", const="vysledky.csv",
                        help="Uloží výsledky do CSV")
    parser.add_argument("--seznam-oboru", action="store_true")
    args = parser.parse_args()

    if args.seznam_oboru:
        print("\nDostupné obory:")
        for k, v in OBORY.items():
            print(f"  {k:<20} → {v}")
        return

    obory      = [o.strip() for o in args.obory.split(",")  if o.strip()]
    uvazek     = [u.strip() for u in args.uvazek.split(",") if u.strip()]
    home_office = [h.strip() for h in args.ho.split(",")   if h.strip()]

    if args.kraj and args.kraj.lower() == "vse":
        print("\nScrapuji všechny kraje ČR...\n")
        nabidky = scrape_vsechny_kraje(
            obory=obory, uvazek=uvazek,
            prima_zamestnavatele=args.prima,
            home_office=home_office,
        )
    else:
        if args.kraj:
            kod = args.kraj.lower()
            if kod not in KRAJE:
                print(f"Neznámá zkratka '{args.kraj}'.", file=sys.stderr)
                sys.exit(1)
            nazev, slug = KRAJE[kod]
        else:
            nazev, slug = "Celá ČR", ""

        print(f"\nScrapuji: {nazev} | max {args.stranky} stran\n")
        nabidky = scrape(
            kraj_slug=slug, kraj_nazev=nazev,
            hledat=args.hledat, obory=obory,
            uvazek=uvazek,
            prima_zamestnavatele=args.prima,
            home_office=home_office,
            max_stranky=args.stranky,
        )

    print()
    tiskni_tabulku(nabidky)

    if args.db:
        from database import init_db, uloz_nabidky
        init_db()
        stats = uloz_nabidky([n.__dict__ for n in nabidky])
        print(f"DB: {stats['nove']} nových, {stats['aktualizovane']} aktualizovaných")

    if args.csv:
        uloz_csv(nabidky, args.csv)


if __name__ == "__main__":
    main()
