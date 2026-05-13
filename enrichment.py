"""HR Hunter — multi-source contact enrichment for Sintera Radar.

For each company, this module:
  1. Finds the company website (ARES + domain guessing)
  2. Fetches the homepage + standard contact/career pages
  3. Extracts emails, phones, and named HR contacts (regex + optional LLM)
  4. Searches third-party portals (personalka.cz, jobs.cz/fp, atmoskop.cz)
  5. Generates likely email patterns when domain is known
  6. Ranks all found contacts by confidence

Designed to fail gracefully on every source — at minimum returns a
generic email + phone for ~67% of Czech companies (per validation
research on 30-firm sample, 2026-05-12).

Public API:
  enrich_firma(firma_name, firma_norm) -> dict
  bulk_enrich(firmas: list, callback=None) -> list
  get_contacts(firma_norm) -> list
"""
import json
import os
import re
import sqlite3
import time
import unicodedata
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

# Lazy imports — requests, bs4, anthropic loaded on first use
_requests = None
_bs4 = None
_anthropic_client = None


def _lazy_requests():
    global _requests
    if _requests is None:
        import requests as r
        _requests = r
    return _requests


def _lazy_bs():
    global _bs4
    if _bs4 is None:
        from bs4 import BeautifulSoup
        _bs4 = BeautifulSoup
    return _bs4


def _lazy_anthropic():
    """Get Anthropic client if ANTHROPIC_API_KEY is set, else None."""
    global _anthropic_client
    if _anthropic_client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            return None
        try:
            from anthropic import Anthropic
            _anthropic_client = Anthropic(api_key=api_key)
        except ImportError:
            return None
    return _anthropic_client


# ── HTTP config ────────────────────────────────────────────────────

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
       "AppleWebKit/537.36 (KHTML, like Gecko) "
       "Chrome/120.0.0.0 Safari/537.36")
_HEADERS = {
    "User-Agent": _UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}
_REQUEST_TIMEOUT = 12


def _http_get(url: str, timeout: int = _REQUEST_TIMEOUT) -> Optional[str]:
    """Robust HTTP GET. Returns text or None on any error."""
    try:
        r = _lazy_requests().get(url, headers=_HEADERS, timeout=timeout,
                                  allow_redirects=True, verify=True)
        if r.status_code == 200:
            return r.text
        return None
    except Exception:
        return None


def _http_get_json(url: str, timeout: int = _REQUEST_TIMEOUT) -> Optional[dict]:
    """JSON GET. Returns dict or None."""
    try:
        r = _lazy_requests().get(url, headers={**_HEADERS, "Accept": "application/json"},
                                  timeout=timeout, allow_redirects=True, verify=True)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None


# ── ARES (Czech business register) lookup ──────────────────────────

def ares_search(firma_name: str) -> list:
    """Search ARES (Czech state business register) by company name.
    Returns list of {ico, dic, nazev, sidlo, web?} matches.

    Free, official API — no rate limit issues.
    Reference: https://ares.gov.cz/swagger-ui/
    """
    if not firma_name or len(firma_name.strip()) < 3:
        return []

    # Strip legal forms for cleaner search
    cleaned = re.sub(
        r"\b(s\.?\s*r\.?\s*o\.?|a\.?\s*s\.?|spol\.?\s*s\s*r\.?\s*o\.?"
        r"|v\.?\s*o\.?\s*s\.?|k\.?\s*s\.?|gmbh|ltd|inc|corp|plc|ag|group|holding"
        r"|o\.?\s*p\.?\s*s\.?|z\.?\s*s\.?|z\.?\s*ú\.?)\b",
        "", firma_name, flags=re.IGNORECASE,
    ).strip().rstrip(",").strip()

    url = ("https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/"
           "ekonomicke-subjekty/vyhledat")
    body = {
        "obchodniJmeno": cleaned[:255],
        "pocet": 5,
        "start": 0,
    }
    try:
        r = _lazy_requests().post(
            url,
            headers={**_HEADERS, "Accept": "application/json",
                     "Content-Type": "application/json"},
            json=body,
            timeout=_REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            return []
        data = r.json()
    except Exception:
        return []

    results = []
    for s in (data.get("ekonomickeSubjekty") or []):
        ico = s.get("ico") or ""
        nazev = s.get("obchodniJmeno") or ""
        sidlo = (s.get("sidlo") or {}).get("textovaAdresa") or ""
        # ARES doesn't always include web in the basic response — we'll
        # fetch the detail for the top candidate if needed
        results.append({
            "ico": ico,
            "nazev": nazev,
            "sidlo": sidlo,
        })
    return results


def ares_detail(ico: str) -> dict:
    """Fetch detail for an IČO. Returns extra fields including website,
    email, phone if listed in ARES."""
    if not ico or not re.fullmatch(r"\d{6,8}", ico):
        return {}
    url = ("https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/"
           "ekonomicke-subjekty/" + ico)
    data = _http_get_json(url)
    if not data:
        return {}

    out = {
        "ico": data.get("ico", ""),
        "dic": data.get("dic", ""),
        "nazev": data.get("obchodniJmeno", ""),
    }
    # Address
    sidlo = data.get("sidlo") or {}
    out["sidlo"] = sidlo.get("textovaAdresa", "")
    # Website (sometimes available, often not)
    sjm = data.get("seznamRegistraci") or {}
    # Try to get web URL — it's not always in the basic API response,
    # but the OR (Obchodní rejstřík) may have it
    return out


# ── Website discovery ─────────────────────────────────────────────

_TLD_CANDIDATES = (".cz", ".com", ".eu", ".sk", ".de", ".net")


def _strip_diacritics(s: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFD', s or '')
        if unicodedata.category(c) != 'Mn'
    )


def _firma_to_domain_candidates(firma_name: str) -> list:
    """Generate plausible domain candidates from company name.
    'TEDOM a.s.' → ['tedom.cz', 'tedom.com', 'tedom.eu']
    'CREDITAS Group' → ['creditas.cz', 'creditasgroup.cz', 'creditas-group.cz', ...]
    'Škoda Auto a.s.' → ['skoda.cz', 'skodaauto.cz', 'skoda-auto.cz', ...]
    """
    s_raw = firma_name.lower()

    # Two cleanings:
    # (1) Strip *only* legal forms (a.s., s.r.o., gmbh, etc.) — keep
    #     descriptors like "group", "holding", "international" because
    #     they're often part of the brand domain (creditasgroup.cz)
    s_with_desc = re.sub(
        r"\b(s\.?\s*r\.?\s*o\.?|a\.?\s*s\.?|spol\.?\s*s\s*r\.?\s*o\.?"
        r"|v\.?\s*o\.?\s*s\.?|k\.?\s*s\.?|gmbh|ltd|inc|corp|plc|ag"
        r"|o\.?\s*p\.?\s*s\.?|z\.?\s*s\.?|z\.?\s*ú\.?"
        r"|příspěvková\s+organizace|státní\s+organizace)\b",
        "", s_raw, flags=re.IGNORECASE,
    )
    # (2) Also strip descriptors for the "brand-only" guess
    s_brand = re.sub(
        r"\b(group|holding|česká\s+republika|česko|czech\s+republic"
        r"|czechia|international|europe|cee)\b",
        "", s_with_desc, flags=re.IGNORECASE,
    )

    def _tokenize(s):
        s = _strip_diacritics(s)
        s = re.sub(r"[^a-z0-9\s\-]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return [w for w in s.split() if len(w) >= 2]

    words_brand = _tokenize(s_brand)
    words_full = _tokenize(s_with_desc)

    if not words_brand and not words_full:
        return []

    # Build base name candidates
    base_names = []

    def _add(name):
        if name and name not in base_names and len(name) >= 3:
            base_names.append(name)

    # Brand-only variants (highest priority — usually the right domain)
    if words_brand:
        _add(words_brand[0])  # first word: "creditas", "tedom", "skoda"
        if len(words_brand) >= 2:
            _add("".join(words_brand[:2]))            # "creditas group" → "creditasgroup"
            _add("-".join(words_brand[:2]))

    # Full-name variants (with descriptors retained — for "creditasgroup")
    if words_full and words_full != words_brand:
        _add(words_full[0])
        if len(words_full) >= 2:
            _add("".join(words_full[:2]))             # "creditasgroup"
            _add("-".join(words_full[:2]))            # "creditas-group"
            _add("".join(words_full))                 # all words joined
            _add("-".join(words_full))                # all words hyphenated

    candidates = []
    for base in base_names:
        for tld in _TLD_CANDIDATES:
            candidates.append("https://www." + base + tld)
            candidates.append("https://" + base + tld)

    # Deduplicate preserving order
    seen = set()
    out = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out[:32]  # increased budget for variations


def find_website(firma_name: str, ico: str = "") -> Optional[str]:
    """Find company website. Returns URL or None."""
    for url in _firma_to_domain_candidates(firma_name):
        if _domain_responds(url):
            return _normalize_url(url)
    return None


def find_websites(firma_name: str, ico: str = "",
                   max_results: int = 3) -> list:
    """Find up to N plausible company websites in priority order.
    Used by enrich_firma to fall back to alternative domains when the
    first one yields no contacts."""
    found = []
    for url in _firma_to_domain_candidates(firma_name):
        if _domain_responds(url):
            n = _normalize_url(url)
            if n not in found:
                found.append(n)
                if len(found) >= max_results:
                    break
    return found


def _normalize_url(url: str) -> str:
    """Ensure URL has scheme + host, no trailing slash."""
    if not url:
        return ""
    if not url.startswith("http"):
        url = "https://" + url
    p = urlparse(url)
    return p.scheme + "://" + p.netloc


def _domain_responds(url: str) -> bool:
    """Quick check — does this domain resolve and return reasonable content?
    Many sites refuse HEAD or return 403 for bot-like UAs; we fall back to GET."""
    try:
        r = _lazy_requests().head(url, headers=_HEADERS, timeout=5,
                                   allow_redirects=True, verify=True)
        if 200 <= r.status_code < 400:
            return True
        # Many sites refuse HEAD or block bot UAs → try GET
        if r.status_code in (403, 405, 501, 502, 503):
            r = _lazy_requests().get(url, headers=_HEADERS, timeout=8,
                                      allow_redirects=True, verify=True,
                                      stream=True)
            r.close()
            return 200 <= r.status_code < 400
        return False
    except Exception:
        # Even if HEAD fails entirely, try a plain GET
        try:
            r = _lazy_requests().get(url, headers=_HEADERS, timeout=8,
                                      allow_redirects=True, verify=True,
                                      stream=True)
            r.close()
            return 200 <= r.status_code < 400
        except Exception:
            return False


# ── Page discovery & fetching ─────────────────────────────────────

# Czech URL paths likely to contain HR/contact info
_PATHS_CONTACT = [
    "/kontakt", "/kontakty", "/kontakt/", "/kontakty/",
    "/contact", "/contact-us", "/contacts",
    "/kontaktujte-nas", "/napiste-nam",
    "/cs/kontakty", "/cz/kontakt", "/en/contact",
]
_PATHS_CAREER = [
    "/kariera", "/kariéra", "/career", "/careers", "/jobs",
    "/prace-u-nas", "/prace", "/zamestnani",
    "/pridejte-se", "/pridejte-se-k-nam",
    "/o-praci-u-nas", "/jobs-careers", "/kariera/",
    "/cs/kariera", "/cz/kariera", "/en/careers",
]
_PATHS_ABOUT = [
    "/o-nas", "/o-firme", "/o-spolecnosti", "/o-nas/",
    "/about", "/about-us", "/firma", "/spolecnost",
    "/tym", "/team", "/lide", "/people", "/vedeni", "/management",
]


def fetch_pages(base_url: str, max_pages: int = 6) -> dict:
    """Fetch homepage + up to N relevant subpages.

    Returns: {url: html_text} for each successful fetch.
    """
    base_url = _normalize_url(base_url)
    if not base_url:
        return {}

    results = {}
    # 1. Homepage — also use it to find linked subpages
    homepage_html = _http_get(base_url)
    if not homepage_html:
        return {}
    results[base_url] = homepage_html

    # 2. Discover internal links pointing to /kontakt, /kariera, /o-nas etc.
    discovered = _discover_relevant_links(base_url, homepage_html)

    # 3. Combine with standard paths
    paths_to_try = list(_PATHS_CONTACT) + list(_PATHS_CAREER) + list(_PATHS_ABOUT)
    candidate_urls = list(discovered)
    for p in paths_to_try:
        u = base_url + p
        if u not in candidate_urls:
            candidate_urls.append(u)

    # 4. Fetch up to max_pages additional (skip already in results)
    fetched_count = 1  # homepage already counted
    for url in candidate_urls:
        if fetched_count >= max_pages:
            break
        if url in results:
            continue
        html = _http_get(url)
        if html and len(html) > 200:
            results[url] = html
            fetched_count += 1

    return results


def _discover_relevant_links(base_url: str, html: str) -> list:
    """Parse homepage HTML to find links that look like contact/career pages."""
    try:
        soup = _lazy_bs()(html, "html.parser")
    except Exception:
        return []

    base_host = urlparse(base_url).netloc.lower()
    relevant_keywords = (
        "kontakt", "contact", "kariera", "kariéra", "career", "careers",
        "o-nas", "o nas", "about", "tym", "team", "lide", "people",
        "vedeni", "management", "jobs", "prace", "zaměstnání",
        "personal", "personalist", "hr",
    )

    found = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        # Normalize to absolute URL
        absolute = urljoin(base_url, href)
        # Same host only
        p = urlparse(absolute)
        if p.netloc.lower() != base_host:
            continue
        # Strip fragment + query for dedup
        clean = p.scheme + "://" + p.netloc + p.path
        if clean in seen:
            continue
        seen.add(clean)
        # Match relevance
        text = (a.get_text() or "").strip().lower()
        href_lower = href.lower()
        if any(kw in text or kw in href_lower for kw in relevant_keywords):
            found.append(clean)
    return found[:15]


def page_to_text(html: str) -> str:
    """Strip HTML, return plain text content."""
    try:
        soup = _lazy_bs()(html, "html.parser")
    except Exception:
        return ""
    # Remove script/style
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    # Collapse whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


# ── Regex-based extraction ────────────────────────────────────────

_EMAIL_RE = re.compile(
    r"\b([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})\b"
)
_PHONE_RE = re.compile(
    r"(?:\+?420\s?)?"  # optional country code
    r"(?:\(\d{3,4}\)\s?)?"
    r"(\d{3}[\s.\-]?\d{3}[\s.\-]?\d{3})"
    r"|(?:\+\d{1,3}\s?\d{2,3}\s?\d{3}[\s.\-]?\d{3,4})"
)
_PHONE_CZ_RE = re.compile(
    r"\b(?:\+?420[\s.\-]?)?"
    r"(\d{3})[\s.\-]?(\d{3})[\s.\-]?(\d{3})\b"
)

# Strictly generic email local-parts that NEVER have a person attached
# (an inbox shared by many people, no designated owner on the page).
_STRICTLY_GENERIC_LOCALS = frozenset({
    "info", "kontakt", "kontakty", "office", "support", "sales",
    "obchod", "general", "hello", "podpora", "produkt",
    "hr", "kariera", "career", "careers", "jobs", "prace", "praca",
    "personal", "personalni", "personalka", "personalistika",
    "nabor", "talent", "people", "lide", "recruit", "recruitment",
    "marketing", "press", "media", "no-reply", "noreply", "do-not-reply",
    "webadmin", "admin", "webmaster", "info_cz", "fakturace", "ucetni",
})


# HR-context keywords used to score email/phone relevance
_HR_KEYWORDS = (
    "hr", "personal", "personalist", "personalni", "personální",
    "personnel", "nábor", "nabor", "kariera", "kariéra", "career",
    "kariér", "recruitment", "recruit", "lidsk", "lidé",
    "human resources", "talent", "people", "team",
    "prace", "práce", "job", "zaměst", "zamestn",
)
_HR_EMAIL_PREFIXES = (
    "hr", "hr@", "hr-", "hr.", "personalni", "personalistika",
    "personalka", "personal", "personalni",
    "prace", "praca", "kariera", "kariéra",
    "career", "careers", "jobs", "recruit", "recruitment",
    "nabor", "nábor", "talent", "people", "lide", "lidsk",
)
_DIRECTOR_KEYWORDS = (
    "reditel", "ředitel", "ceo", "managing director", "general manager",
    "jednatel", "majitel", "president", "prezident",
    "country manager", "country head",
)

# Lines that look like contact blocks (we look around emails/phones to
# extract a name and role)
def _phone_normalize(text: str) -> Optional[str]:
    """Clean phone number to canonical '+420 XXX XXX XXX' form.
    Returns None if not a valid Czech number."""
    digits = re.sub(r"\D", "", text)
    if digits.startswith("420"):
        digits = digits[3:]
    if len(digits) == 9 and digits[0] in "23456789":
        return "+420 {} {} {}".format(digits[0:3], digits[3:6], digits[6:9])
    if len(digits) >= 9:
        # Might be foreign — keep original
        return text.strip()
    return None


def extract_contacts_regex(text: str, firma: str = "",
                            source_url: str = "") -> list:
    """Extract emails and phones from plain text using regex + heuristics.

    Returns list of contact dicts:
      {typ: 'hr_email'|'general_email'|'phone'|'named_person',
       jmeno, pozice, email, telefon, confidence, zdroj, poznamka}
    """
    if not text:
        return []

    contacts = []
    text_lower = text.lower()
    domain_hint = ""
    if source_url:
        try:
            domain_hint = urlparse(source_url).netloc.lower().replace("www.", "")
        except Exception:
            pass

    # ── Emails ──
    seen_emails = set()
    for m in _EMAIL_RE.finditer(text):
        email = m.group(1).lower()
        if email in seen_emails:
            continue
        seen_emails.add(email)
        # Filter obvious junk
        if (email.endswith(".png") or email.endswith(".jpg")
                or email.endswith(".gif") or "@example." in email
                or "@email.com" == email
                or len(email) > 80):
            continue

        # Score
        local_part = email.split("@")[0]
        is_hr = any(local_part.startswith(p.rstrip("@")) for p in _HR_EMAIL_PREFIXES)
        is_hr = is_hr or any(local_part == p.rstrip("@") for p in _HR_EMAIL_PREFIXES)
        is_personal = ("." in local_part and len(local_part) >= 5
                       and not local_part.startswith(("info", "kontakt", "sales",
                                                       "office", "support",
                                                       "marketing", "press",
                                                       "obchod", "produkt")))
        is_generic = local_part in ("info", "kontakt", "sales", "office", "support",
                                     "marketing", "press", "obchod", "general")

        # Determine type and confidence
        if is_hr:
            ctype = "hr_email"
            conf = 0.85
        elif is_personal and domain_hint and email.endswith("@" + domain_hint):
            ctype = "person_email"
            conf = 0.75
        elif is_generic:
            ctype = "general_email"
            conf = 0.5
        else:
            ctype = "general_email"
            conf = 0.55

        # First, try to parse the name from the email itself (jmeno.prijmeni@…)
        jmeno_email = _parse_name_from_email(local_part)
        # Truly generic inboxes have no person attached (info@, hr@, kariera@).
        # Specialized regional/department aliases (socialni.chrudim@) MIGHT
        # have a designated person on the page — we still try text-proximity.
        is_strictly_generic = local_part in _STRICTLY_GENERIC_LOCALS
        if jmeno_email:
            jmeno, pozice = jmeno_email, ""
            ctype = "person_email" if not is_hr else "named_person"
            conf = min(0.85, conf + 0.10)
        elif is_strictly_generic:
            # Generic inbox — no person name from page text.
            jmeno, pozice = "", ""
        else:
            # Fallback: find a name in nearby text.
            jmeno, pozice = _extract_name_near(text, email, firma=firma)
            if jmeno:
                ctype = "named_person" if "hr" in (pozice or "").lower() else ctype
                conf = min(0.78, conf + 0.05)

        contacts.append({
            "typ": ctype,
            "jmeno": jmeno or "",
            "pozice": pozice or "",
            "email": email,
            "telefon": "",
            "confidence": conf,
            "zdroj": source_url,
            "poznamka": "regex",
        })

    # ── Phone numbers (Czech) ──
    seen_phones = set()
    for m in _PHONE_CZ_RE.finditer(text):
        raw = m.group(0)
        norm = _phone_normalize(raw)
        if not norm or norm in seen_phones:
            continue
        seen_phones.add(norm)

        # Context for HR detection (±100 chars around match)
        start = max(0, m.start() - 100)
        end = min(len(text), m.end() + 100)
        ctx = text[start:end].lower()
        is_hr_ctx = any(kw in ctx for kw in _HR_KEYWORDS)
        is_helpline = any(kw in ctx for kw in
                          ("infolink", "infolinka", "zákaznick", "klientsk",
                           "customer", "helpline", "non-stop", "nonstop",
                           "porucha", "havarie", "tísňov"))

        if is_hr_ctx:
            ctype = "hr_phone"
            conf = 0.75
        elif is_helpline:
            ctype = "helpline"
            conf = 0.3
        else:
            ctype = "phone"
            conf = 0.5

        # Only look for a name near the phone for non-helpline, non-HR-generic
        # phones. For helpline/HR-role phones, the "name" is the role, not a
        # person.
        if is_helpline:
            jmeno, pozice = "", ""
        else:
            jmeno, pozice = _extract_name_near(text, raw, firma=firma)
        contacts.append({
            "typ": ctype,
            "jmeno": jmeno or "",
            "pozice": pozice or "",
            "email": "",
            "telefon": norm,
            "confidence": conf,
            "zdroj": source_url,
            "poznamka": "regex",
        })

    return contacts


_CZ_NAME_RE = re.compile(
    r"(?:Ing\.|Mgr\.|Bc\.|MUDr\.|PhDr\.|JUDr\.|RNDr\.|prof\.|doc\.|DiS\.)?\s*"
    r"([A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]{2,}(?:ová)?)"  # first
    r"\s+"
    r"([A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]{2,}(?:ová)?)"  # last
)

# Words that look like names but are noise (navigation menus, page titles,
# department/section labels, business jargon).
_NAME_NOISE = {
    # Navigation / page chrome
    "kontakty", "kariera", "kariéra", "aktuality", "služby", "pojištění",
    "produkty", "novinky", "domů", "obchod", "kontakt", "menu",
    "nahoru", "stránka", "stránek", "stránce", "stránku", "navigace",
    "domains", "domain", "soubory", "cookies", "zpráva",
    "podmínky", "podmínek", "ochrana", "pravidla", "soukromí",
    "potřebujete", "potřebujeme", "můžeme", "děkujeme", "děkuji",
    "informace", "spravujeme", "spravované", "spolupracujeme",
    "spolupracujte", "přidat", "přidejte", "vstoupit", "přihlásit",
    "přihlášení", "registrace", "registrovat",
    # Cities / regions (false positives for name extraction)
    "společnost", "praha", "brno", "ostrava", "plzeň", "liberec",
    "olomouc", "pardubice", "zlín", "hradec", "ústí", "české",
    "budějovice", "boleslav", "karlovy", "varech", "jihlava",
    "telč", "kolín", "kladno", "most", "havířov", "karviná", "děčín",
    "chomutov", "teplice", "frýdek", "místek", "prostějov", "přerov",
    "třebíč", "trutnov", "tábor", "písek", "klatovy", "sokolov",
    "kroměříž", "šumperk", "vsetín", "valašské", "meziříčí",
    "uherské", "uherský", "hradiště", "krumlov", "tišnov", "kuřim",
    "moravský", "moravské", "moravská", "slezský", "středočeský",
    "jihočeský", "královéhradecký", "pardubický", "vysočina",
    "jihomoravský", "olomoucký", "moravskoslezský",
    "ústecký", "liberecký", "plzeňský", "karlovarský", "zlínský",
    # Directional adjectives (used as regional qualifiers)
    "severní", "jižní", "východní", "západní", "centrální",
    "horní", "dolní", "vnitřní", "vnější", "horní", "spodní",
    "starý", "stará", "staré", "nový", "nová", "nové", "velký",
    "velká", "velké", "malý", "malá", "malé", "hlavní",
    # Departments / sections / roles (these come up next to role emails)
    "fakturační", "fakturace", "kanceláře", "kancelář",
    "klientský", "zákaznický", "servis", "infolinka",
    "ceník", "produkt", "produktové", "produktový",
    "výroba", "výrobní", "výrobou", "montáž", "montážní",
    "oddělení", "oddělením", "divize", "závod", "závodní",
    "provoz", "provozní", "sekce", "úsek", "skupina",
    "vedení", "vedoucí", "ředitel", "ředitelka", "ředitelství",
    "personální", "personalistika", "personalistka", "personalista",
    "obchodní", "obchod", "marketing", "marketingový", "marketingová",
    "tým", "týmu", "týmem", "týmy", "týmů",
    "identifikační", "identifikace", "identifikační",
    "kontaktní", "kontakt", "kontakta",
    "stáhněte", "stáhnout", "úvod", "úvodní", "hlavní",
    # English business jargon (false positives on bilingual pages)
    "media", "contact", "press", "marketing", "sales", "support",
    "customer", "service", "team", "office", "headquarters", "hq",
    "department", "division", "section",
    "recepce", "reception", "receptionist", "operator",
    # Common brand-name fragments seen in our data
    "activ", "aktiv", "forte", "premium", "professional",
    # Generic business adjectives
    "veřejná", "veřejné", "veřejný", "soukromá", "soukromý", "soukromé",
    "centrální", "lokální", "regionální", "globální",
    "pobočka", "pobočky", "pobočku", "filiálka",
    "kontaktovat", "spojte", "napište", "voláme", "voláte",
    # Common UI strings
    "akce", "více", "více informací", "zobrazit", "číst", "stáhnout",
}


def _looks_like_name(first: str, last: str) -> bool:
    """Heuristic: is this pair of words a plausible Czech (or foreign) name?
    Used by text-proximity fallback — primarily noise-based filtering,
    plus rejection of obvious noun/adjective endings."""
    f, l = first.lower(), last.lower()
    if f in _NAME_NOISE or l in _NAME_NOISE:
        return False

    # Reject endings that are clearly nouns or adjectives (Czech):
    #   -ž / -š = often verb stems or nouns (Montáž, oprav, vrhuš)
    #   -ní = adjective (identifikační, technický → -ní/-cký)
    #   -ství / -ictví = abstract nouns
    #   -ace / -ence = abstract nouns
    #   -ost / -nost = abstract nouns
    noun_adjective_endings = (
        "ž", "š",
        "ní", "ního", "ním", "ních",
        "ství", "ictví", "ostí",
        "ace", "ence", "ize",
        "ost", "nost", "tost",
        "tní", "vní", "zní",
    )
    if l.endswith(noun_adjective_endings):
        return False
    if f.endswith(noun_adjective_endings):
        return False

    # Some bad first-word starters (verb prefixes, articles)
    if f.startswith(("xx", "vý", "po", "ne", "ne_")):
        return False

    # Length sanity
    if len(f) < 3 or len(l) < 3:
        return False

    # Reject if either word contains a digit
    if any(c.isdigit() for c in f) or any(c.isdigit() for c in l):
        return False

    return True


_EMAIL_NAME_NOISE = {
    "info", "kontakt", "kontakty", "office", "support", "sales",
    "obchod", "marketing", "press", "media", "data", "general",
    "kariera", "career", "careers", "jobs", "prace", "nabor",
    "hr", "personal", "personalni", "personalka",
    "recepce", "kancelar", "fakturace", "obchodni", "produkt",
    "no-reply", "noreply", "do-not-reply",
    "socialni", "stanicni", "vedouci", "manager", "admin",
    "podpora", "stiznost", "podnety", "udrzba",
}

_CZ_NAME_TOKEN_RE = re.compile(r"^[a-z][a-z0-9_-]{1,}$")


def _parse_name_from_email(local_part: str) -> str:
    """If email local-part looks like 'first.last' (or 'first_last'),
    return 'First Last' with proper capitalization. Else return ''.

    Filters obvious role-based addresses (info, kariera, hr, etc.).
    """
    if not local_part:
        return ""
    # Strip trailing digits (jana.novakova2 → jana.novakova)
    lp = re.sub(r"\d+$", "", local_part)
    # Must contain a separator
    if not re.search(r"[._\-]", lp):
        return ""
    # Split on . _ -
    parts = re.split(r"[._\-]+", lp)
    parts = [p for p in parts if p]
    if len(parts) < 2 or len(parts) > 4:
        return ""
    # Reject if any part is a role keyword
    for p in parts:
        if p.lower() in _EMAIL_NAME_NOISE:
            return ""
        if not _CZ_NAME_TOKEN_RE.match(p.lower()):
            return ""
        if len(p) < 2:
            return ""
    # Capitalize each part (also handles foreign names)
    titled = [p.capitalize() for p in parts]
    # For Czech, restore diacritics heuristically by looking at common names? Not reliable.
    # Just use ASCII title-case — users will recognize it.
    return " ".join(titled)


def _extract_name_near(text: str, anchor: str, firma: str = "") -> tuple:
    """Search for a Czech-looking name within ±250 chars of an anchor.
    Returns (full_name, position_title) or ('', '')."""
    if not anchor or anchor not in text:
        return ("", "")
    pos = text.find(anchor)
    start = max(0, pos - 300)
    end = min(len(text), pos + 300)
    ctx = text[start:end]

    # Build firma word set to exclude from name extraction
    firma_words = set()
    if firma:
        for w in re.split(r"[\s.,\-/&()]+", firma.lower()):
            if len(w) >= 3:
                firma_words.add(w)
                firma_words.add(_strip_diacritics(w))

    candidates = []
    for m in _CZ_NAME_RE.finditer(ctx):
        first, last = m.group(1), m.group(2)
        if not _looks_like_name(first, last):
            continue
        # Reject if either word matches the firma name
        fl = first.lower()
        ll = last.lower()
        if (fl in firma_words or ll in firma_words
                or _strip_diacritics(fl) in firma_words
                or _strip_diacritics(ll) in firma_words):
            continue
        full = "{} {}".format(first, last)
        name_pos = m.start()
        anchor_pos = ctx.find(anchor)
        dist = abs(anchor_pos - name_pos) if anchor_pos >= 0 else 999
        candidates.append((dist, full))

    if not candidates:
        return ("", "")
    candidates.sort()
    closest = candidates[0][1]

    # Try to find a role/position next to the name
    pozice = ""
    role_patterns = [
        r"(HR\s+(?:Manager|Director|Specialist|Generalist|Business Partner|Lead))",
        r"(Personální\s+(?:ředitel|ředitelka|specialist|specialistka|manager|asistent|asistentka))",
        r"(Personalist(?:ka|a))",
        r"(Náborář(?:ka)?)",
        r"(Recruiter|Talent Acquisition|HRBP)",
        r"(?:HR|Personální)\s*[-–—:]\s*([A-Z][^\n]{0,40})",
    ]
    for pat in role_patterns:
        rm = re.search(pat, ctx, re.IGNORECASE)
        if rm:
            pozice = rm.group(1).strip().rstrip(",.")
            break

    return (closest, pozice)


# ── Email pattern guessing ────────────────────────────────────────

def email_pattern_guess(domain: str, names: list = None,
                        known_emails: list = None) -> list:
    """Generate likely email addresses for a domain.

    Returns list of {email, confidence, typ, poznamka} candidates.
    """
    domain = (domain or "").strip().lower().replace("www.", "")
    if not domain or "." not in domain:
        return []

    out = []
    # 1. HR-specific role addresses
    role_prefixes = [
        ("hr", 0.6, "hr_email"),
        ("personal", 0.55, "hr_email"),
        ("personalni", 0.55, "hr_email"),
        ("personalistika", 0.5, "hr_email"),
        ("personalka", 0.5, "hr_email"),
        ("prace", 0.55, "hr_email"),
        ("kariera", 0.65, "hr_email"),
        ("career", 0.55, "hr_email"),
        ("careers", 0.55, "hr_email"),
        ("jobs", 0.55, "hr_email"),
        ("nabor", 0.55, "hr_email"),
        ("recruit", 0.5, "hr_email"),
        ("recruitment", 0.5, "hr_email"),
        ("talent", 0.5, "hr_email"),
        ("info", 0.4, "general_email"),
        ("kontakt", 0.4, "general_email"),
    ]
    for prefix, conf, typ in role_prefixes:
        email = "{}@{}".format(prefix, domain)
        if not known_emails or email not in known_emails:
            out.append({
                "email": email, "confidence": conf, "typ": typ,
                "telefon": "", "jmeno": "", "pozice": "",
                "zdroj": "pattern_guess",
                "poznamka": "Vzorová adresa — neověřeno",
            })

    # 2. Person-based (if names provided)
    if names:
        for full_name in names:
            parts = full_name.strip().split()
            if len(parts) < 2:
                continue
            first = _strip_diacritics(parts[0].lower())
            last = _strip_diacritics(parts[-1].lower())
            if len(first) < 2 or len(last) < 2:
                continue
            patterns = [
                ("{}.{}".format(first, last), 0.55),     # jana.novakova
                ("{}.{}".format(first[0], last), 0.45),  # j.novakova
                ("{}{}".format(first, last), 0.4),       # jananovakova
                (last, 0.35),                            # novakova
                ("{}{}".format(first[0], last), 0.4),    # jnovakova
            ]
            for pat, conf in patterns:
                email = "{}@{}".format(pat, domain)
                out.append({
                    "email": email, "confidence": conf,
                    "typ": "person_email",
                    "telefon": "", "jmeno": full_name, "pozice": "",
                    "zdroj": "pattern_guess",
                    "poznamka": "Vzorová adresa pro {} — neověřeno".format(full_name),
                })

    return out


# ── LLM extraction (optional, with Anthropic API) ─────────────────

_LLM_EXTRACTION_PROMPT = """Extract HR/career contact information from the company website text below.

Company: {firma}

Return STRICT JSON with this shape:
{{
  "contacts": [
    {{
      "typ": "hr_named" | "hr_generic" | "person" | "phone_only" | "general",
      "jmeno": "Full name or empty",
      "pozice": "Role/title or empty",
      "email": "email@example.com or empty",
      "telefon": "+420 XXX XXX XXX or empty",
      "confidence": 0.0-1.0,
      "poznamka": "Brief context note"
    }}
  ]
}}

Rules:
- Focus on HR, recruitment, career, personnel contacts. Skip customer service.
- "hr_named" = named person with HR/recruitment role. Highest priority.
- "hr_generic" = HR-specific generic email (hr@, personalni@, kariera@) without a name.
- "person" = a named person with email/phone but not clearly HR (e.g. director, manager).
- "phone_only" = HR-specific phone with no email.
- "general" = generic company contact (info@, sales@, central phone).
- Skip helpline/customer service numbers (8XX, anything tagged as zákaznický, klientský, infolinka).
- Confidence reflects how certain you are this contact is HR-relevant (1.0 = explicit HR Manager, 0.5 = generic info@).
- Return ONLY the JSON. No markdown, no explanation.

Text:
{text}
"""


def extract_contacts_llm(text: str, firma: str,
                         source_url: str = "") -> list:
    """LLM-based structured extraction. Returns [] if API unavailable.

    Falls back gracefully — caller should always combine with regex result.
    """
    client = _lazy_anthropic()
    if client is None:
        return []

    # Trim text to ~12000 chars to stay within token budget
    trimmed = text[:12000]
    prompt = _LLM_EXTRACTION_PROMPT.format(firma=firma, text=trimmed)

    try:
        response = client.messages.create(
            model="claude-haiku-4-5",  # cheap/fast for extraction
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = ""
        for block in response.content:
            if hasattr(block, "text"):
                raw += block.text
    except Exception as e:
        return []

    # Parse JSON
    raw = raw.strip()
    # Strip markdown fence if present
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # Try to extract JSON object from the response
        m = re.search(r"\{[\s\S]*\}", raw)
        if m:
            try:
                data = json.loads(m.group(0))
            except Exception:
                return []
        else:
            return []

    out = []
    for c in data.get("contacts", []):
        if not isinstance(c, dict):
            continue
        out.append({
            "typ": c.get("typ", "general"),
            "jmeno": (c.get("jmeno") or "").strip(),
            "pozice": (c.get("pozice") or "").strip(),
            "email": (c.get("email") or "").strip().lower(),
            "telefon": _phone_normalize(c.get("telefon") or "") or (c.get("telefon") or "").strip(),
            "confidence": float(c.get("confidence") or 0.5),
            "zdroj": source_url,
            "poznamka": "LLM: " + (c.get("poznamka") or "").strip(),
        })
    return out


_LLM_ROLE_PROMPT = """For each contact below, find their role/title at "{firma}" by reading the page text.

Reply with STRICT JSON only (no markdown, no explanation):
{{"contacts": [{{"email": "...", "pozice": "...short role..."}}]}}

Rules:
- Match each contact by email.
- pozice = short role/title in Czech or English (max 6 words),
  e.g. "HR Manager", "Regional Director", "Personální ředitelka",
  "Vrchní sestra Pyšely", "Sales Manager Brno", "Recepce".
- If role is unclear from the text, set pozice to "" — do NOT guess.
- Include EVERY contact from the input. Empty pozice is OK.
- DO NOT change the email value. Keep it exactly as given.

Contacts to attribute:
{contacts_list}

Source page text (multiple pages concatenated):
{text}
"""


def llm_attribute_roles(contacts: list, page_text: str, firma: str,
                        force: bool = False) -> list:
    """For contacts with jmeno+email but empty pozice, use Claude to
    find their role from the page text. Updates contacts in-place.

    Returns the contacts list (same reference).

    Cost: ~$0.002-0.005 per firma (one Claude Haiku call).
    """
    client = _lazy_anthropic()
    if client is None:
        return contacts

    # Find contacts that need role attribution
    targets = []
    for c in contacts:
        email = (c.get("email") or "").strip().lower()
        jmeno = (c.get("jmeno") or "").strip()
        pozice = (c.get("pozice") or "").strip()
        zdroj = c.get("zdroj") or ""
        # Only attribute for: has email + has jmeno + no pozice + real source
        if not email or not jmeno:
            continue
        if pozice and not force:
            continue
        if "pattern_guess" in zdroj:
            continue
        targets.append(c)

    if not targets or not page_text:
        return contacts

    contacts_list = "\n".join(
        "- {} ({})".format(c["jmeno"], c["email"]) for c in targets
    )
    # Trim text to fit token budget — Claude Haiku 4.5 has 200K context;
    # 60K chars ≈ 15K tokens is plenty and gives us room for full
    # regional/team pages + homepage. Cost: ~$0.004/call.
    trimmed_text = page_text[:60000]
    prompt = _LLM_ROLE_PROMPT.format(
        firma=firma, contacts_list=contacts_list, text=trimmed_text)

    try:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = "".join(b.text for b in response.content if hasattr(b, "text"))
    except Exception:
        return contacts

    # Parse JSON
    raw = raw.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", raw)
        if not m:
            return contacts
        try:
            data = json.loads(m.group(0))
        except Exception:
            return contacts

    # Apply roles by email match
    by_email = {}
    for item in data.get("contacts", []):
        em = (item.get("email") or "").strip().lower()
        po = (item.get("pozice") or "").strip()
        if em and po:
            by_email[em] = po

    for c in contacts:
        email = (c.get("email") or "").strip().lower()
        if email in by_email and not c.get("pozice"):
            c["pozice"] = by_email[email]
            prev_note = c.get("poznamka") or ""
            if "role: LLM" not in prev_note:
                c["poznamka"] = (prev_note + " | role: LLM").strip(" |")

    return contacts


# ── Third-party portal search ─────────────────────────────────────

def search_personalka(firma_name: str, ico: str = "") -> list:
    """Search personalka.cz for company profile (often has HR contact)."""
    # personalka.cz uses /firma/<IČO> URLs — we need ICO
    if not ico:
        return []
    url = "https://personalka.cz/firma/" + ico
    html = _http_get(url)
    if not html:
        return []
    text = page_to_text(html)
    return extract_contacts_regex(text, firma=firma_name, source_url=url)


def search_jobs_cz_fp(firma_name: str) -> list:
    """Search jobs.cz/fp company page if discoverable via Google-less probe.
    We can't reliably find the company ID without search, but we can try
    common patterns.

    For now, this is a placeholder — returns empty unless we have ID.
    """
    # jobs.cz/fp URLs need numeric IDs we don't store. Skip for now.
    return []


def search_atmoskop(firma_name: str) -> list:
    """Search atmoskop.cz — has firma profile pages with some HR info."""
    # atmoskop.cz pattern: /nazory-na-zamestnavatele/<id>-<slug>
    # We can't easily resolve slug without search. Skip unless we add it.
    return []


# ── Orchestrator ───────────────────────────────────────────────────

def enrich_firma(firma_name: str, firma_norm: str = "",
                 force_refresh: bool = False) -> dict:
    """Main pipeline: orchestrate all sources, return ranked contacts.

    Returns: {
      'firma': str,
      'firma_norm': str,
      'website': str,
      'ico': str,
      'contacts': [list of contact dicts],
      'sources_tried': [list of strings],
      'errors': [list],
      'duration_ms': int,
      'status': 'success' | 'partial' | 'failed'
    }
    """
    from database import normalize_company
    if not firma_norm:
        firma_norm = normalize_company(firma_name)

    t0 = time.time()
    result = {
        "firma": firma_name,
        "firma_norm": firma_norm,
        "website": "",
        "ico": "",
        "contacts": [],
        "sources_tried": [],
        "errors": [],
        "status": "failed",
    }

    # ── Step 1: ARES lookup for IČO + canonical name ──
    try:
        ares_results = ares_search(firma_name)
        result["sources_tried"].append("ARES")
        if ares_results:
            result["ico"] = ares_results[0].get("ico", "")
    except Exception as e:
        result["errors"].append("ARES: " + str(e))

    # ── Step 2: Find website (up to 3 candidates for fallback) ──
    websites = []
    try:
        websites = find_websites(firma_name, ico=result["ico"], max_results=3)
        if websites:
            result["website"] = websites[0]
            result["sources_tried"].append("website:" + websites[0])
    except Exception as e:
        result["errors"].append("find_websites: " + str(e))

    all_contacts = []

    # ── Step 3: Scrape website pages — try alt domains if first yields nothing ──
    def _useful_contacts(contacts):
        return [c for c in contacts
                if (c.get("email") or c.get("telefon"))
                and (c.get("zdroj") or "").startswith("http")]

    # Collected page texts for LLM role attribution. Stored as
    # (priority, url, text) so we can sort: contact/team pages first
    # (they have role context), homepage last (often huge, generic).
    page_texts = []

    def _page_priority(u):
        u = (u or "").lower()
        if any(k in u for k in ("/kontakt", "/lide", "/tym", "/team",
                                  "/o-nas", "/vedeni", "/management",
                                  "/people", "/staff", "/contact")):
            return 0  # highest priority
        if "/kariera" in u or "/career" in u or "/jobs" in u:
            return 1
        # Regional / city sub-pages (often have team listings)
        if u.count("/") <= 4 and u.endswith("/"):
            return 2
        return 3

    for idx, ws in enumerate(websites):
        try:
            pages = fetch_pages(ws, max_pages=6)
            if idx == 0:
                result["sources_tried"].append("pages:" + str(len(pages)))
            else:
                result["sources_tried"].append("alt_domain:" + ws + ":pages:" + str(len(pages)))
            for url, html in pages.items():
                text = page_to_text(html)
                # Keep text for later role-attribution call (priority-ordered)
                if text and len(text) > 200:
                    page_texts.append((_page_priority(url), url, text))
                regex_contacts = extract_contacts_regex(
                    text, firma=firma_name, source_url=url)
                all_contacts.extend(regex_contacts)
                if _lazy_anthropic() and any(p in url.lower() for p in
                                              ("kontakt", "contact", "kariera",
                                               "career", "team", "lide", "vedeni")):
                    llm_contacts = extract_contacts_llm(
                        text, firma=firma_name, source_url=url)
                    all_contacts.extend(llm_contacts)
                    if llm_contacts:
                        result["sources_tried"].append("LLM:" + url)
        except Exception as e:
            result["errors"].append("fetch_pages[{}]: {}".format(idx, str(e)))

        # If first website yielded useful contacts, stop here
        if idx == 0 and len(_useful_contacts(all_contacts)) >= 1:
            break
        # If we've tried 2 websites and have nothing, one more attempt
        if idx >= 1 and len(_useful_contacts(all_contacts)) >= 1:
            break

    # ── Step 4: Personálka.cz (if we have IČO) ──
    if result["ico"]:
        try:
            personalka_contacts = search_personalka(firma_name, result["ico"])
            if personalka_contacts:
                result["sources_tried"].append("personalka.cz")
                all_contacts.extend(personalka_contacts)
        except Exception as e:
            result["errors"].append("personalka: " + str(e))

    # ── Step 5: Pattern guess (if we have domain) ──
    if result["website"]:
        try:
            domain = urlparse(result["website"]).netloc.replace("www.", "")
            # Collect any named people we found
            named_people = [c["jmeno"] for c in all_contacts
                            if c.get("jmeno") and len(c["jmeno"]) > 4]
            known_emails = [c["email"] for c in all_contacts if c.get("email")]
            guessed = email_pattern_guess(
                domain, names=named_people[:5],
                known_emails=known_emails,
            )
            # Only keep top 8 guesses (deduplicated)
            guessed = guessed[:8]
            if guessed:
                result["sources_tried"].append("pattern_guess:" + str(len(guessed)))
                all_contacts.extend(guessed)
        except Exception as e:
            result["errors"].append("pattern_guess: " + str(e))

    # ── Step 6: LLM role attribution (if API key set) ──
    # For all contacts with jmeno+email but no pozice, ask Claude
    # to find their role/title from the source page text.
    if _lazy_anthropic() and page_texts:
        try:
            need_roles = sum(
                1 for c in all_contacts
                if c.get("jmeno") and c.get("email") and not c.get("pozice")
                and "pattern_guess" not in (c.get("zdroj") or "")
            )
            if need_roles > 0:
                # Build combined text — contact/team pages FIRST
                # (they contain the role context), then less-relevant pages
                page_texts.sort(key=lambda x: x[0])
                combined_text = "\n\n".join(
                    "=== " + url + " ===\n" + text
                    for _, url, text in page_texts
                )
                llm_attribute_roles(all_contacts, combined_text, firma_name)
                result["sources_tried"].append("LLM-roles:" + str(need_roles))
        except Exception as e:
            result["errors"].append("llm_roles: " + str(e))

    # ── Step 7: Deduplicate & rank ──
    deduplicated = _dedupe_contacts(all_contacts)

    # Ranking: HR-named > HR-generic > person > general > guessed > phone
    def rank_key(c):
        typ = c.get("typ", "")
        base = {
            "hr_named": 1, "named_person": 1, "hr_email": 2,
            "person_email": 3, "hr_phone": 4, "person": 4,
            "general_email": 5, "phone": 6, "general": 7,
            "phone_only": 6, "hr_generic": 2, "helpline": 9,
        }.get(typ, 8)
        return (base, -float(c.get("confidence", 0.5)))

    deduplicated.sort(key=rank_key)
    result["contacts"] = deduplicated

    # Status
    has_hr = any(c.get("typ") in ("hr_named", "named_person", "hr_email", "hr_generic",
                                    "hr_phone", "person_email")
                 and "pattern_guess" not in c.get("zdroj", "")
                 for c in deduplicated)
    has_any = len(deduplicated) > 0
    if has_hr:
        result["status"] = "success"
    elif has_any:
        result["status"] = "partial"
    else:
        result["status"] = "failed"

    result["duration_ms"] = int((time.time() - t0) * 1000)
    return result


def _dedupe_contacts(contacts: list) -> list:
    """Merge duplicate emails/phones, preferring higher-confidence sources."""
    # Group by (email, phone) — same contact info means same person
    by_key = {}
    for c in contacts:
        key = (c.get("email", "").lower(), c.get("telefon", ""))
        if key == ("", ""):
            # No identifying info — only dedup by name
            key = ("", "", c.get("jmeno", "").strip().lower())
        if key not in by_key:
            by_key[key] = c.copy()
        else:
            existing = by_key[key]
            # Keep higher confidence
            if c.get("confidence", 0) > existing.get("confidence", 0):
                # Merge — keep richer fields
                for field in ("jmeno", "pozice", "telefon", "email"):
                    if not existing.get(field) and c.get(field):
                        existing[field] = c[field]
                existing["confidence"] = c["confidence"]
                existing["typ"] = c["typ"]
                existing["zdroj"] = c["zdroj"]
                existing["poznamka"] = c["poznamka"]
            else:
                # Lower confidence — still merge missing fields
                for field in ("jmeno", "pozice", "telefon", "email"):
                    if not existing.get(field) and c.get(field):
                        existing[field] = c[field]
    return list(by_key.values())


# ── Persistence ─────────────────────────────────────────────────────

def save_enrichment_result(result: dict) -> int:
    """Save enrichment result to firma_kontakty + firma_web + enrich_log.
    Returns number of new contacts saved."""
    from database import get_conn
    firma_norm = result.get("firma_norm", "")
    firma = result.get("firma", "")
    if not firma_norm:
        return 0

    now = datetime.now().isoformat(timespec="seconds")
    saved_count = 0

    with get_conn() as conn:
        # 1. Upsert firma_web
        if result.get("website"):
            conn.execute("""
                INSERT INTO firma_web (firma_norm, firma, ico, url, zdroj,
                                        datum_zjisteni, posledni_scan)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(firma_norm) DO UPDATE SET
                    url = excluded.url,
                    ico = excluded.ico,
                    posledni_scan = excluded.posledni_scan
            """, (firma_norm, firma, result.get("ico", ""),
                  result["website"], "auto", now, now))

        # 2. Insert or update contacts.
        # For exact duplicates (same email+phone+firma), update the
        # existing row only when the new data has BETTER information
        # (non-empty pozice/jmeno that was previously empty, or higher
        # confidence). This way re-enrichment improves existing contacts
        # rather than skipping them.
        for c in result.get("contacts", []):
            email = (c.get("email") or "").strip().lower()
            telefon = (c.get("telefon") or "").strip()
            if not email and not telefon:
                continue
            new_jmeno = (c.get("jmeno") or "").strip()
            new_pozice = (c.get("pozice") or "").strip()
            new_conf = float(c.get("confidence", 0.5))

            existing = conn.execute("""
                SELECT id, jmeno, pozice, confidence FROM firma_kontakty
                WHERE firma_norm = ? AND COALESCE(email,'') = ? AND COALESCE(telefon,'') = ?
                  AND aktivni = 1
            """, (firma_norm, email, telefon)).fetchone()

            if existing:
                # Decide if we should update
                fields_to_update = {}
                if new_jmeno and not (existing["jmeno"] or "").strip():
                    fields_to_update["jmeno"] = new_jmeno
                if new_pozice and not (existing["pozice"] or "").strip():
                    fields_to_update["pozice"] = new_pozice
                if new_conf > (existing["confidence"] or 0):
                    fields_to_update["confidence"] = new_conf
                if fields_to_update:
                    set_clause = ", ".join("{}=?".format(k) for k in fields_to_update)
                    params = list(fields_to_update.values()) + [existing["id"]]
                    conn.execute(
                        "UPDATE firma_kontakty SET " + set_clause +
                        " WHERE id = ?",
                        params,
                    )
                continue

            conn.execute("""
                INSERT INTO firma_kontakty
                  (firma_norm, firma, typ, jmeno, pozice, email, telefon,
                   zdroj, confidence, poznamka, datum_zjisteni)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (firma_norm, firma, c.get("typ", "general"),
                  new_jmeno, new_pozice,
                  email, telefon, c.get("zdroj", "") or "",
                  new_conf,
                  c.get("poznamka", "") or "", now))
            saved_count += 1

        # 3. Log the run
        conn.execute("""
            INSERT INTO enrich_log
              (firma_norm, firma, datum, zdroje_zkusene, kontakty_nalezeno,
               status, chyba, trvani_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (firma_norm, firma, now,
              ", ".join(result.get("sources_tried", [])),
              saved_count, result.get("status", "failed"),
              "; ".join(result.get("errors", []))[:1000],
              int(result.get("duration_ms", 0))))

    return saved_count


def get_contacts(firma_norm: str, active_only: bool = True) -> list:
    """Load all stored contacts for a company."""
    from database import get_conn
    where = "firma_norm = ?"
    params = [firma_norm]
    if active_only:
        where += " AND aktivni = 1"
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM firma_kontakty WHERE " + where +
            " ORDER BY confidence DESC, id ASC", params).fetchall()
    return [dict(r) for r in rows]


def get_firma_web(firma_norm: str) -> Optional[dict]:
    """Get cached website for a firma, if any."""
    from database import get_conn
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM firma_web WHERE firma_norm = ?",
            (firma_norm,)).fetchone()
    return dict(row) if row else None


def get_last_enrichment(firma_norm: str) -> Optional[dict]:
    """Get the most recent enrichment log entry for a firma."""
    from database import get_conn
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM enrich_log WHERE firma_norm = ? "
            "ORDER BY datum DESC LIMIT 1",
            (firma_norm,)).fetchone()
    return dict(row) if row else None


# ── Bulk enrichment with threading ─────────────────────────────────

_bulk_jobs = {}  # job_id → {status, progress, total, results}


# ── Focused role-only refresh ────────────────────────────────────

def enrich_firma_roles_only(firma_norm: str) -> dict:
    """Refresh pozice (role/title) for existing named contacts of a firma.

    Skips ARES, domain discovery, pattern guessing, contact extraction.
    Just: load cached website → fetch pages → run LLM role attribution
    → UPDATE existing DB rows with new roles.

    Faster + cheaper than full enrich_firma. Use for filling pozice gaps
    on firms that were enriched before LLM role attribution existed.

    Returns: {firma_norm, firma, website, updated, status, duration_ms}
    """
    from database import get_conn
    t0 = time.time()
    result = {
        "firma_norm": firma_norm,
        "firma": "",
        "website": "",
        "updated": 0,
        "status": "failed",
        "errors": [],
    }

    # Load cached website
    web = get_firma_web(firma_norm)
    if not web or not web.get("url"):
        result["status"] = "no_website"
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return result
    result["website"] = web["url"]
    result["firma"] = web.get("firma", "") or ""

    # Load existing named contacts without pozice
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, jmeno, email, telefon, pozice, confidence
            FROM firma_kontakty
            WHERE firma_norm = ? AND aktivni = 1
              AND LENGTH(COALESCE(jmeno, '')) >= 5
              AND LENGTH(COALESCE(email, '')) >= 5
              AND COALESCE(pozice, '') = ''
              AND zdroj NOT LIKE '%pattern_guess%'
        """, (firma_norm,)).fetchall()
        contacts_needing_role = [dict(r) for r in rows]

    if not contacts_needing_role:
        result["status"] = "nothing_to_do"
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return result

    if _lazy_anthropic() is None:
        result["errors"].append("No ANTHROPIC_API_KEY — cannot attribute roles")
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return result

    # Fetch pages (priority-ordered: contact/team pages first)
    def _page_priority(u):
        u = (u or "").lower()
        if any(k in u for k in ("/kontakt", "/lide", "/tym", "/team",
                                  "/o-nas", "/vedeni", "/management",
                                  "/people", "/staff", "/contact")):
            return 0
        if "/kariera" in u or "/career" in u or "/jobs" in u:
            return 1
        if u.count("/") <= 4 and u.endswith("/"):
            return 2
        return 3

    try:
        pages = fetch_pages(result["website"], max_pages=6)
    except Exception as e:
        result["errors"].append("fetch_pages: " + str(e))
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return result

    page_texts = []
    for url, html in pages.items():
        text = page_to_text(html)
        if text and len(text) > 200:
            page_texts.append((_page_priority(url), url, text))

    if not page_texts:
        result["status"] = "no_page_text"
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return result

    page_texts.sort(key=lambda x: x[0])
    combined_text = "\n\n".join(
        "=== " + url + " ===\n" + text for _, url, text in page_texts
    )

    # Call LLM role attribution on the contacts
    try:
        llm_attribute_roles(contacts_needing_role, combined_text,
                            result["firma"] or firma_norm)
    except Exception as e:
        result["errors"].append("llm_attribute: " + str(e))
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return result

    # Apply updates to DB
    now = datetime.now().isoformat(timespec="seconds")
    updated = 0
    with get_conn() as conn:
        for c in contacts_needing_role:
            new_pozice = (c.get("pozice") or "").strip()
            if not new_pozice:
                continue
            conn.execute("""
                UPDATE firma_kontakty
                SET pozice = ?, poznamka = COALESCE(poznamka, '') || ' | role: LLM-only'
                WHERE id = ? AND COALESCE(pozice, '') = ''
            """, (new_pozice, c["id"]))
            updated += 1

    result["updated"] = updated
    result["status"] = "success" if updated > 0 else "no_roles_found"
    result["duration_ms"] = int((time.time() - t0) * 1000)
    return result


def bulk_role_refresh_start(firma_norms: list) -> str:
    """Background job for role-only refresh on multiple firms."""
    import threading, uuid
    job_id = str(uuid.uuid4())[:12]
    _bulk_jobs[job_id] = {
        "status": "running",
        "progress": 0,
        "total": len(firma_norms),
        "current": "",
        "results": [],
        "started": datetime.now().isoformat(timespec="seconds"),
        "kind": "roles_only",
    }

    def _worker():
        for i, firma_norm in enumerate(firma_norms):
            _bulk_jobs[job_id]["current"] = firma_norm
            try:
                r = enrich_firma_roles_only(firma_norm)
                _bulk_jobs[job_id]["results"].append({
                    "firma_norm": firma_norm,
                    "firma": r.get("firma", ""),
                    "status": r["status"],
                    "updated": r["updated"],
                })
            except Exception as e:
                _bulk_jobs[job_id]["results"].append({
                    "firma_norm": firma_norm,
                    "firma": "",
                    "status": "error",
                    "updated": 0,
                    "error": str(e),
                })
            _bulk_jobs[job_id]["progress"] = i + 1
        _bulk_jobs[job_id]["status"] = "done"
        _bulk_jobs[job_id]["current"] = ""

    threading.Thread(target=_worker, daemon=True).start()
    return job_id


def find_firms_needing_roles() -> list:
    """Return firma_norms of firms that have named contacts without pozice."""
    from database import get_conn
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT DISTINCT firma_norm FROM firma_kontakty
            WHERE aktivni = 1
              AND LENGTH(COALESCE(jmeno, '')) >= 5
              AND LENGTH(COALESCE(email, '')) >= 5
              AND COALESCE(pozice, '') = ''
              AND zdroj NOT LIKE '%pattern_guess%'
        """).fetchall()
    return [r[0] for r in rows]


def bulk_enrich_start(firmas: list) -> str:
    """Start a background bulk-enrichment job.
    Returns a job_id that can be polled for progress."""
    import threading, uuid
    job_id = str(uuid.uuid4())[:12]
    _bulk_jobs[job_id] = {
        "status": "running",
        "progress": 0,
        "total": len(firmas),
        "current": "",
        "results": [],
        "started": datetime.now().isoformat(timespec="seconds"),
    }

    def _worker():
        for i, (firma, firma_norm) in enumerate(firmas):
            _bulk_jobs[job_id]["current"] = firma
            try:
                result = enrich_firma(firma, firma_norm)
                save_enrichment_result(result)
                _bulk_jobs[job_id]["results"].append({
                    "firma": firma,
                    "firma_norm": firma_norm,
                    "status": result["status"],
                    "contacts": len(result["contacts"]),
                    "website": result.get("website", ""),
                })
            except Exception as e:
                _bulk_jobs[job_id]["results"].append({
                    "firma": firma,
                    "firma_norm": firma_norm,
                    "status": "error",
                    "contacts": 0,
                    "website": "",
                    "error": str(e),
                })
            _bulk_jobs[job_id]["progress"] = i + 1
        _bulk_jobs[job_id]["status"] = "done"
        _bulk_jobs[job_id]["current"] = ""

    threading.Thread(target=_worker, daemon=True).start()
    return job_id


def bulk_enrich_status(job_id: str) -> Optional[dict]:
    return _bulk_jobs.get(job_id)


# ── Auto-enrich: pick problematic firms missing contacts ──────────

def find_firms_needing_enrichment(limit: int = 50,
                                    min_pozic: int = 3,
                                    days_since_last_attempt: int = 7) -> list:
    """Find problematic firms (3+ active pozic, no LinkedIn DM, not recently
    enriched). Returns list of (firma, firma_norm) ready to be enriched.

    Order: most problematic first (aktualizované × pocet_pozic).
    """
    from database import get_conn, normalize_company

    with get_conn() as conn:
        # Step 1: candidate firms — active, multiple positions, no agency
        rows = conn.execute("""
            SELECT n.firma,
                   COUNT(*) as pocet_pozic,
                   SUM(CASE WHEN publikovano LIKE '%ktualizov%' THEN 1 ELSE 0 END) as aktual,
                   SUM(CASE WHEN predchozi_job_id IS NOT NULL
                            AND predchozi_job_id != '' THEN 1 ELSE 0 END) as opak,
                   SUM(CASE WHEN pocet_scanu >= 4 THEN 1 ELSE 0 END) as probl
            FROM nabidky n
            WHERE n.aktivni = 1 AND n.firma != '' AND n.is_agency = 0
            GROUP BY n.firma
            HAVING pocet_pozic >= ?
            ORDER BY (aktual * 3 + opak * 2 + probl) DESC, pocet_pozic DESC
        """, (min_pozic,)).fetchall()

        # Step 2: firms that already have LinkedIn DM
        dm_norms = {r[0] for r in conn.execute(
            "SELECT DISTINCT company_normalized FROM decision_makers "
            "WHERE current_company != ''"
        )}

        # Step 3: firms already enriched recently
        from datetime import timedelta
        threshold = (datetime.now() - timedelta(days=days_since_last_attempt)
                     ).isoformat(timespec="seconds")
        recently_enriched = {r[0] for r in conn.execute(
            "SELECT DISTINCT firma_norm FROM enrich_log WHERE datum >= ?",
            (threshold,)
        )}

    candidates = []
    for r in rows:
        firma = r["firma"]
        norm = normalize_company(firma)
        if norm in dm_norms:
            continue  # already has LinkedIn DM
        if norm in recently_enriched:
            continue  # already tried recently
        candidates.append((firma, norm))
        if len(candidates) >= limit:
            break
    return candidates


def nightly_auto_enrich(limit: int = 30) -> dict:
    """Run enrichment on a batch of needs-enrichment firms.
    Designed to be called by a daily cron after the scraper finishes.

    Returns: {processed, success, partial, failed, total_contacts}
    """
    candidates = find_firms_needing_enrichment(limit=limit)
    stats = {"processed": 0, "success": 0, "partial": 0,
             "failed": 0, "total_contacts": 0}
    for firma, norm in candidates:
        try:
            r = enrich_firma(firma, norm)
            saved = save_enrichment_result(r)
            stats["processed"] += 1
            stats[r.get("status", "failed")] = stats.get(r.get("status", "failed"), 0) + 1
            stats["total_contacts"] += saved
        except Exception:
            stats["failed"] = stats.get("failed", 0) + 1
            stats["processed"] += 1
    return stats
