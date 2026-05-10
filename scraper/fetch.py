"""
Harris County Motivated Seller Lead Scraper - Final Production Version
- Scrapes RP.aspx via Playwright using confirmed exact field IDs
- Parses results table using RP-YYYY-NNNNN file number pattern
- Enriches with HCAD owners.txt + real_acct.txt from Google Drive
- Exports records.json + GHL CSV
"""

import asyncio
import json
import csv
import io
import os
import re
import sys
import time
import zipfile
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from enrichment import EnrichmentEngine
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

try:
    from dbfread import DBF
except ImportError:
    DBF = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

CLERK_URL       = "https://www.cclerk.hctx.net/applications/websearch/RP.aspx"
GDRIVE_ZIP_ID   = "1wV4EW-uxasZUjkc_wxOr-UjVKUTK5-qh"  # Real_Account_Owner.zip
FILE_NUMBER_RE  = re.compile(r'^RP-\d{4}-\d+$')

# Harris County actual instrument codes from Codes.aspx
DOC_TYPES = {
    "L/P":    ("lp",      "Lis Pendens"),
    "NOTICE": ("fc",      "Notice of Foreclosure / Trustee Sale"),
    "TRSALE": ("fc",      "Trustee Sale"),
    "LIEN":   ("lien",    "Lien"),
    "T/L":    ("lien",    "Federal Tax Lien"),
    "JUDGE":  ("jud",     "Judgment"),
    "A/J":    ("jud",     "Abstract of Judgment"),
    "PROB":   ("probate", "Probate"),
    "DEED":   ("tax",     "Deed (Sheriff/Trustee/Tax)"),
    "BNKRCY": ("lien",    "Bankruptcy"),
    "LEVY":   ("lien",    "Notice of Levy"),
    "REL":    ("lp",      "Release"),
}

SCORE_BASE        = 30
SCORE_PER_FLAG    = 10
SCORE_LP_FC_COMBO = 20
SCORE_AMOUNT_100K = 15
SCORE_AMOUNT_50K  = 10
SCORE_NEW_WEEK    = 5
SCORE_HAS_ADDRESS = 5

OUTPUT_DIRS = [Path("dashboard"), Path("data")]


# ─── HCAD Parcel Lookup ───────────────────────────────────────────────────────
class HCADParcelLookup:
    """
    Builds two lookups from HCAD bulk data:
    1. acct_to_addr: acct# → {site_addr, mail_addr, ...} from real_acct.txt
    2. name_to_acct: normalized owner name → acct# from owners.txt
    
    owners.txt columns (tab-delimited):
        acct | ln_num | name | aka | pct_own
    
    real_acct.txt columns (tab-delimited):
        acct | yr | mailto_addr | mail_addr_2 | mail_city | mail_state | 
        mail_zip | mail_country | ... | site_addr | ...
    """

    def __init__(self):
        self.name_lookup: dict = {}   # normalized name → parcel dict
        self.acct_lookup: dict = {}   # acct# → parcel dict

    def _normalize(self, name: str) -> str:
        return re.sub(r"\s+", " ", (name or "").strip().upper())

    def _name_variants(self, name: str) -> list:
        n = self._normalize(name)
        parts = n.split()
        variants = [n]
        if len(parts) >= 2:
            variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")
            variants.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
        return variants

    def _download_gdrive(self) -> Optional[bytes]:
        """Download zip from Google Drive handling large file warning."""
        log.info("Downloading HCAD zip from Google Drive...")
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})

        # Try usercontent direct download with confirm=t
        try:
            url = (f"https://drive.usercontent.google.com/download"
                   f"?id={GDRIVE_ZIP_ID}&export=download&confirm=t")
            r = session.get(url, timeout=300)
            if r.status_code == 200 and r.content[:2] == b'PK':
                log.info(f"  ✓ {len(r.content)/1e6:.1f} MB downloaded")
                return r.content
        except Exception as e:
            log.warning(f"  usercontent failed: {e}")

        # Fallback: get page, extract uuid, confirm
        try:
            r = session.get(
                f"https://drive.google.com/uc?export=download&id={GDRIVE_ZIP_ID}",
                timeout=60)
            m = re.search(r'name="uuid" value="([^"]+)"', r.text)
            if m:
                uuid = m.group(1)
                r2 = session.get(
                    f"https://drive.usercontent.google.com/download"
                    f"?id={GDRIVE_ZIP_ID}&export=download&confirm=t&uuid={uuid}",
                    timeout=300)
                if r2.status_code == 200 and r2.content[:2] == b'PK':
                    log.info(f"  ✓ {len(r2.content)/1e6:.1f} MB (uuid confirm)")
                    return r2.content
        except Exception as e:
            log.warning(f"  uuid confirm failed: {e}")

        log.warning("  Could not download from Google Drive")
        return None

    def _parse_txt_from_zip(self, zip_bytes: bytes,
                             filename_hint: str) -> Optional[io.StringIO]:
        """Extract a specific txt file from zip by name hint."""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                names = z.namelist()
                log.info(f"  Zip contents: {names}")
                match = next(
                    (n for n in names
                     if filename_hint.lower() in n.lower()), None)
                if not match:
                    # Try any txt file
                    match = next(
                        (n for n in names
                         if n.lower().endswith(".txt")), None)
                if match:
                    log.info(f"  Reading: {match}")
                    with z.open(match) as f:
                        return io.StringIO(
                            f.read().decode("latin-1", errors="replace"))
        except Exception as e:
            log.warning(f"  Zip read error: {e}")
        return None

    def _load_real_acct(self, zip_bytes: bytes) -> dict:
        """
        Parse real_acct.txt → acct_to_parcel dict.
        Tab-delimited. Columns confirmed from screenshot:
        acct, yr, mailto_addr, mail_addr_2, mail_city, mail_state, mail_zip,
        mail_country, ...many cols..., site_addr, ...
        """
        acct_map = {}
        sio = self._parse_txt_from_zip(zip_bytes, "real_acct")
        if not sio:
            return acct_map

        reader = csv.DictReader(sio, delimiter="\t")
        # Normalize fieldnames
        raw_fields = reader.fieldnames or []
        fields = [f.strip().lower() for f in raw_fields]
        log.info(f"  real_acct fields: {fields[:20]}")

        count = 0
        for row in reader:
            try:
                r = {k.strip().lower(): (v or "").strip()
                     for k, v in row.items() if k}
                acct = r.get("acct", "").strip()
                if not acct:
                    continue

                # Mailing address - confirmed field names from log
                mail_addr  = r.get("mail_addr_1") or r.get("mailto") or ""
                mail_city  = r.get("mail_city", "")
                mail_state = r.get("mail_state", "TX")
                mail_zip   = r.get("mail_zip", "")

                # Site/property address - build from components
                # site_addr_1 = street number + name, site_addr_2 = city, site_addr_3 = zip
                site_addr  = r.get("site_addr_1", "").strip()
                site_city  = r.get("site_addr_2", "") or "HOUSTON"
                site_state = "TX"
                site_zip   = r.get("site_addr_3", "")
                
                # Filter out "0" addresses (no street number)
                if site_addr and site_addr.startswith("0 "):
                    # Build from street components instead
                    num = r.get("str_num", "").strip()
                    if num and num != "0":
                        parts = [num, r.get("str_pfx",""), r.get("str",""),
                                 r.get("str_sfx",""), r.get("str_sfx_dir",""),
                                 r.get("str_unit","")]
                        site_addr = " ".join(p for p in parts if p).strip()
                    else:
                        site_addr = ""
                elif not site_addr:
                    num = r.get("str_num", "").strip()
                    if num and num != "0":
                        parts = [num, r.get("str_pfx",""), r.get("str",""),
                                 r.get("str_sfx",""), r.get("str_sfx_dir",""),
                                 r.get("str_unit","")]
                        site_addr = " ".join(p for p in parts if p).strip()
                if not site_addr:
                    parts = [
                        r.get("str_num",""), r.get("str_pfx",""),
                        r.get("str",""), r.get("str_sfx",""),
                        r.get("str_sfx_dir",""), r.get("str_unit","")
                    ]
                    site_addr = " ".join(p for p in parts if p).strip()

                acct_map[acct] = {
                    "mail_addr":  mail_addr,
                    "mail_city":  mail_city,
                    "mail_state": mail_state,
                    "mail_zip":   mail_zip,
                    "site_addr":  site_addr,
                    "site_city":  site_city,
                    "site_state": site_state,
                    "site_zip":   site_zip,
                }
                count += 1
            except Exception:
                continue

        log.info(f"  Loaded {count} real_acct records")
        return acct_map

    def _load_owners(self, zip_bytes: bytes, acct_map: dict):
        """
        Parse owners.txt → build name_lookup.
        Tab-delimited. Columns confirmed from screenshot:
        acct | ln_num | name | aka | pct_own
        """
        sio = self._parse_txt_from_zip(zip_bytes, "owner")
        if not sio:
            return

        reader = csv.DictReader(sio, delimiter="\t")
        raw_fields = reader.fieldnames or []
        fields = [f.strip().lower() for f in raw_fields]
        log.info(f"  owners fields: {fields[:10]}")

        count = 0
        for row in reader:
            try:
                r = {k.strip().lower(): (v or "").strip()
                     for k, v in row.items() if k}
                acct = r.get("acct", "").strip()
                name = r.get("name", "").strip()
                aka  = r.get("aka", "").strip()

                if not acct or not name:
                    continue

                parcel = acct_map.get(acct)
                if not parcel:
                    continue

                # Build name lookup
                for n in [name, aka]:
                    if n:
                        for variant in self._name_variants(n):
                            self.name_lookup[variant] = parcel

                self.acct_lookup[acct] = parcel
                count += 1
            except Exception:
                continue

        log.info(f"  Loaded {count} owner records → "
                 f"{len(self.name_lookup)} name keys")

    def load(self):
        zip_bytes = self._download_gdrive()
        if not zip_bytes:
            log.warning("HCAD unavailable — no address enrichment")
            return

        log.info("Parsing HCAD data files...")
        acct_map = self._load_real_acct(zip_bytes)
        if not acct_map:
            log.warning("  real_acct parse returned 0 records")
            return

        # Parse parcel_tieback for legal description matching
        parcel_tieback = self._load_parcel_tieback(zip_bytes)

        # Build enrichment engine
        self.engine = EnrichmentEngine()
        owners = self._get_owners_raw(zip_bytes)
        self.engine.build_from_hcad(owners, acct_map, parcel_tieback)
        self._load_owners(zip_bytes, acct_map)
    def _load_parcel_tieback(self, zip_bytes: bytes) -> dict:
        """
        Parse parcel_tieback.txt for legal description matching.
        Confirmed columns: acct | tp | dscr | related_acct | pct
        The dscr field contains text like:
          "FOXWOOD SEC 4 LT 15 BLK 4"
          "SUNDOWN GLEN SEC 6 LT 23 BLK 6"
        We parse this using the same legal description parser.
        """
        from enrichment import parse_legal_description, legal_match_key
        result = {}
        sio = self._parse_txt_from_zip(zip_bytes, "parcel_tieback")
        if not sio:
            return result
        reader = csv.DictReader(sio, delimiter="\t")
        fields = [f.strip().lower() for f in (reader.fieldnames or [])]
        log.info(f"  parcel_tieback fields: {fields[:10]}")
        parsed_count = 0
        for row in reader:
            try:
                r    = {k.strip().lower(): (v or "").strip()
                        for k, v in row.items() if k}
                acct = r.get("acct", "").strip()
                dscr = r.get("dscr", "").strip()
                if not acct or not dscr:
                    continue
                # Parse the dscr text field into components
                # dscr format: "FOXWOOD SEC 4 LT 15 BLK 4"
                # Need to normalize to match clerk format
                dscr_normalized = dscr.upper()
                # Replace common abbreviations to match our parser
                dscr_normalized = dscr_normalized.replace(" LT ", " LOT ").replace(" LTS ", " LOT ")
                dscr_normalized = dscr_normalized.replace(" BLK ", " BLOCK ").replace(" BK ", " BLOCK ")
                # Add "Desc:" prefix so our parser picks it up
                # Log first 5 dscr values to understand format
                if parsed_count == 0 and len(result) < 10:
                    log.info(f"  dscr sample: {repr(dscr)}")
                dscr_for_parse = "Desc: " + dscr_normalized
                parsed = parse_legal_description(dscr_for_parse)
                key = legal_match_key(parsed)
                if key:
                    result[acct] = parsed
                    parsed_count += 1
                else:
                    # Store raw dscr for fallback matching
                    result[acct] = {
                        "subdivision": dscr_normalized.split()[0] if dscr_normalized else "",
                        "section": "", "lot": "", "block": "",
                        "dscr_raw": dscr_normalized
                    }
            except Exception:
                continue
        log.info(f"  Parcel tieback: {len(result)} records, {parsed_count} with full legal key")
        return result

    def _get_owners_raw(self, zip_bytes: bytes) -> list:
        records = []
        sio = self._parse_txt_from_zip(zip_bytes, "owner")
        if not sio:
            return records
        reader = csv.DictReader(sio, delimiter="\t")
        for row in reader:
            records.append(dict(row))
        return records

    def find(self, name: str) -> Optional[dict]:
        if not name:
            return None
        for v in self._name_variants(name):
            if v in self.name_lookup:
                return self.name_lookup[v]
        return None


# ─── Scoring ──────────────────────────────────────────────────────────────────
def build_flags(rec: dict, today: datetime) -> list:
    flags = []
    try:
        cat      = rec.get("cat", "")
        doc_type = rec.get("doc_type", "").upper()
        owner    = (rec.get("owner") or "").upper()
        filed    = rec.get("filed") or ""

        if cat == "lp" and "REL" not in doc_type:
            flags.append("Lis pendens")
        if cat == "fc":
            if rec.get("source") == "FRCL":
                flags.append("Foreclosure auction scheduled")
                flags.append("Pre-foreclosure")
            else:
                flags.append("Pre-foreclosure")
        if cat == "jud":
            flags.append("Judgment lien")
        if doc_type in ("T/L", "LEVY"):
            flags.append("Tax lien")
        if cat == "lien" and doc_type == "LIEN":
            flags.append("Mechanic lien")
        if cat == "probate":
            flags.append("Probate / estate")
        if any(x in owner for x in
               ("LLC", " LP", "INC", "CORP", "LTD", "TRUST", "ASSOC")):
            flags.append("LLC / corp owner")
        try:
            dt = datetime.strptime(filed[:10], "%Y-%m-%d")
            if (today - dt).days <= 7:
                flags.append("New this week")
        except Exception:
            pass
    except Exception:
        pass
    return flags


def compute_score(rec: dict, flags: list) -> int:
    try:
        score = SCORE_BASE + len(flags) * SCORE_PER_FLAG
        cat = rec.get("cat", "")
        if "Lis pendens" in flags and "Pre-foreclosure" in flags:
            score += SCORE_LP_FC_COMBO
        elif cat in ("lp", "fc") and len(flags) >= 2:
            score += SCORE_LP_FC_COMBO
        amount = 0
        try:
            amount = float(str(rec.get("amount") or "0")
                           .replace(",", "").replace("$", ""))
        except Exception:
            pass
        if amount >= 100_000:
            score += SCORE_AMOUNT_100K
        elif amount >= 50_000:
            score += SCORE_AMOUNT_50K
        if "New this week" in flags:
            score += SCORE_NEW_WEEK
        if rec.get("prop_address"):
            score += SCORE_HAS_ADDRESS
        return min(score, 100)
    except Exception:
        return SCORE_BASE


# ─── Harris County Clerk Scraper ──────────────────────────────────────────────
class HarrisClerkScraper:
    """
    Playwright scraper for RP.aspx.
    
    Confirmed from debug logs:
    - Field IDs: ctl00_ContentPlaceHolder1_txtInstrument / txtFrom / txtTo
    - Results: 137 tables per page, main table has header row with
      "File Number | File Date | Type Vol Page | Names | Legal | Pgs | Film Code"
    - File numbers match pattern: RP-YYYY-NNNNN
    - Names cell has lines: "Grantor : NAME" and "Grantee : NAME"
    """

    BASE_URL = "https://www.cclerk.hctx.net/applications/websearch/RP.aspx"

    def __init__(self, days_back: int = 7):
        self.days_back  = days_back
        self.today      = datetime.now()
        self.start_date = self.today - timedelta(days=days_back)
        self.records: list = []

    def _parse_results_page(self, html: str, doc_code: str,
                            cat: str, cat_label: str) -> list:
        soup = BeautifulSoup(html, "lxml")
        rows_out = []

        # Harris County renders 137+ tables per page.
        # Strategy: find ALL tds whose text matches RP-YYYY-NNNNN,
        # then walk up to the parent tr and extract sibling cells.
        # This is more robust than finding the "header table" which
        # may not be detected correctly.
        
        # First try: find header table approach
        target_table = None
        for tbl in soup.find_all("table"):
            txt = tbl.get_text(" ", strip=True)
            if ("File Number" in txt and "File Date" in txt
                    and FILE_NUMBER_RE.search(txt)):
                target_table = tbl
                break

        if target_table:
            all_trs = target_table.find_all("tr")
        else:
            # Fallback: collect all trs from the entire page
            all_trs = soup.find_all("tr")

        for tr in all_trs:
            try:
                cells = tr.find_all("td")
                if len(cells) < 5:
                    continue

                # Find file number — debug showed it can be at offset 1 (empty first cell)
                # e.g. Row cells: ['', 'RP-2026-130137', '04/07/2026', 'L/P', ...]
                file_num = ''
                file_idx = -1
                for _ci, _cell in enumerate(cells):
                    _txt = _cell.get_text(strip=True)
                    if FILE_NUMBER_RE.match(_txt):
                        file_num = _txt
                        file_idx = _ci
                        break

                if not file_num or file_idx < 0:
                    continue

                def _gc(offset):
                    idx = file_idx + offset
                    return cells[idx] if idx < len(cells) else None

                # Cell +1: File Date
                raw_date = _gc(1).get_text(strip=True) if _gc(1) else ''
                filed_iso = ''
                for fmt in ('%m/%d/%Y', '%Y-%m-%d'):
                    try:
                        filed_iso = datetime.strptime(
                            raw_date[:10], fmt).strftime('%Y-%m-%d')
                        break
                    except Exception:
                        pass

                # Cell +2: Instrument type
                type_cell = _gc(2)
                type_link = type_cell.find('a') if type_cell else None
                rec_type  = type_link.get_text(strip=True) if type_link else doc_code

                # Cell +3: Names — names are in nested sub-tables inside this cell
                # Structure: outer td contains inner tables with Grantor/Grantee rows
                name_cell = _gc(3)
                grantor   = ''
                grantees  = []
                if name_cell:
                    # Get all text, including from nested tables
                    full_text = name_cell.get_text(' ', strip=True)
                    
                    # Try nested table approach first — each row has label + name
                    inner_rows = name_cell.find_all('tr')
                    for irow in inner_rows:
                        icells = irow.find_all('td')
                        if len(icells) >= 2:
                            label = icells[0].get_text(strip=True).lower()
                            name  = icells[1].get_text(strip=True)
                            if 'grantor' in label and name:
                                grantor = name
                            elif 'grantee' in label and name:
                                grantees.append(name)
                    
                    # Fallback: parse flat text
                    # Debug showed: "Grantor:MELCHOR GRACIELAGrantee:TREVINO"
                    # Split on Grantor/Grantee keywords
                    if not grantor:
                        # Insert newlines before keywords
                        tagged = re.sub(r'(Grantor|Grantee)', r'\n\1', full_text, flags=re.IGNORECASE)
                        for line2 in tagged.split('\n'):
                            line2 = line2.strip()
                            if not line2:
                                continue
                            low2 = line2.lower()
                            if low2.startswith('grantor'):
                                val = re.sub(r'^grantor\s*[:\s]\s*', '', line2, flags=re.IGNORECASE).strip()
                                if val:
                                    grantor = val
                            elif low2.startswith('grantee'):
                                val = re.sub(r'^grantee\s*[:\s]\s*', '', line2, flags=re.IGNORECASE).strip()
                                if val:
                                    grantees.append(val)
                # Cell +4: Legal description
                legal_cell = _gc(4)
                legal_raw_text = legal_cell.get_text(' ', strip=True) if legal_cell else ''
                # Clean up legal desc — strip any leaked Grantor/Grantee text
                import re as _re
                legal = _re.sub(r'^(Grantor|Grantee)\s*[:\s]\s*', '', legal_raw_text, flags=_re.IGNORECASE).strip()

                # Last cell: Film Code link
                clerk_url = self.BASE_URL
                a = cells[-1].find('a', href=True)

                if a:
                    href = a.get("href", "")
                    clerk_url = (href if href.startswith("http")
                                 else "https://www.cclerk.hctx.net" + href)

                rows_out.append({
                    "doc_num":   file_num,
                    "doc_type":  rec_type or doc_code,
                    "filed":     filed_iso or raw_date,
                    "cat":       cat,
                    "cat_label": cat_label,
                    "owner":     grantor,
                    "grantee":   ", ".join(grantees),
                    "contact":   (grantees[0] if (cat == "lp" and grantees) else grantor),
                    "amount":    None,
                    "legal":     legal,
                    "clerk_url": clerk_url,
                })

            except Exception as e:
                log.warning(f"Row parse error: {e}")
                continue

        return rows_out

    async def _search_one(self, page, doc_code: str,
                          cat: str, cat_label: str):
        log.info(f"  → {doc_code} ({cat_label})")

        for attempt in range(3):
            try:
                await page.goto(self.BASE_URL,
                                wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2000)
                break
            except PWTimeout:
                log.warning(f"    Timeout (attempt {attempt+1})")
                if attempt == 2:
                    return

        try:
            date_from = self.start_date.strftime("%m/%d/%Y")
            date_to   = self.today.strftime("%m/%d/%Y")

            # Confirmed exact field IDs from debug log
            await page.fill(
                '#ctl00_ContentPlaceHolder1_txtInstrument', doc_code)
            await page.fill(
                '#ctl00_ContentPlaceHolder1_txtFrom', date_from)
            await page.fill(
                '#ctl00_ContentPlaceHolder1_txtTo', date_to)

            log.info(f"    Filled: {doc_code} | {date_from} → {date_to}")

            await page.click('input[value="Search"]')
            await page.wait_for_load_state("domcontentloaded", timeout=20000)
            await page.wait_for_timeout(2000)

            page_num = 0
            while True:
                page_num += 1
                html = await page.content()
                # Debug: dump page snapshot on first L/P search only
                if page_num == 1 and doc_code == "L/P":
                    from bs4 import BeautifulSoup as _BS
                    _s = _BS(html, "lxml")
                    # Log first RP- match in full page text
                    _txt = _s.get_text(" ")
                    _idx = _txt.find("RP-20")
                    if _idx >= 0:
                        log.info(f"    FOUND RP- at index {_idx}: ...{_txt[_idx-50:_idx+200]}...")
                    else:
                        log.info(f"    NO RP- pattern in page text")
                    # Log all table count and first table with RP-
                    _tables = _s.find_all("table")
                    log.info(f"    Tables on page: {len(_tables)}")
                    for _i, _t in enumerate(_tables):
                        if "RP-20" in _t.get_text():
                            log.info(f"    First table with RP-: index {_i}")
                            # Log its rows
                            _rows = _t.find_all("tr")
                            log.info(f"    That table has {len(_rows)} rows")
                            for _r in _rows[:3]:
                                _cells = _r.find_all("td")
                                log.info(f"    Row cells ({len(_cells)}): {[_c.get_text(strip=True)[:30] for _c in _cells[:5]]}")
                            break
                rows = self._parse_results_page(html, doc_code, cat, cat_label)
                log.info(f"    Page {page_num}: {len(rows)} rows")
                self.records.extend(rows)

                next_btn = await page.query_selector(
                    'input[value="NEXT"], input[value="Next"], '
                    'a:has-text("NEXT"), a:has-text("Next")')
                if not next_btn:
                    break
                await next_btn.click()
                await page.wait_for_load_state("domcontentloaded", timeout=15000)
                await page.wait_for_timeout(1000)

        except Exception as e:
            log.warning(f"  Error on {doc_code}: {e}")

    async def scrape_all(self) -> list:
        log.info(f"Scraping: {self.start_date.date()} → {self.today.date()}")
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"),
                viewport={"width": 1280, "height": 900},
            )
            page = await ctx.new_page()
            page.set_default_timeout(30000)

            for doc_code, (cat, cat_label) in DOC_TYPES.items():
                try:
                    await self._search_one(page, doc_code, cat, cat_label)
                except Exception as e:
                    log.error(f"Fatal error on {doc_code}: {e}")


            # Also scrape dedicated Foreclosure portal (FRCL_R.aspx)
            try:
                await self._scrape_frcl(page)
            except Exception as e:
                log.error(f"FRCL scrape error: {e}")

            await browser.close()

        log.info(f"Total clerk records: {len(self.records)}")
        return self.records

    async def _scrape_frcl(self, page) -> None:
        """
        Scrape the dedicated Foreclosure portal (FRCL_R.aspx).
        Searches by Sale Date for current + next month (upcoming auctions)
        and by File Date for last 30 days (recently filed).
        """
        FRCL_URL = "https://www.cclerk.hctx.net/applications/websearch/FRCL_R.aspx"
        log.info("  → FRCL_R (Foreclosure Portal)")

        month_names = {
            1: "January", 2: "February", 3: "March", 4: "April",
            5: "May", 6: "June", 7: "July", 8: "August",
            9: "September", 10: "October", 11: "November", 12: "December"
        }

        # Determine months to cover: prev month, current, next
        now = self.today
        months_to_search = []
        # Previous month (covers 30-day lookback)
        if now.month == 1:
            months_to_search.append((now.year - 1, 12))
        else:
            months_to_search.append((now.year, now.month - 1))
        # Current month
        months_to_search.append((now.year, now.month))
        # Next month (upcoming auctions)
        if now.month == 12:
            months_to_search.append((now.year + 1, 1))
        else:
            months_to_search.append((now.year, now.month + 1))

        frcl_count = 0
        for year, month in months_to_search:
            for search_type in ["Sale Date", "File Date"]:
                try:
                    await page.goto(FRCL_URL, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(2500)

                    # Debug: dump all input/select elements on first visit
                    if frcl_count == 0 and search_type == "Sale Date":
                        inputs = await page.eval_on_selector_all(
                            "input, select",
                            "els => els.map(e => ({tag: e.tagName, id: e.id, name: e.name, type: e.type || '', value: e.value || ''}))"
                        )
                        log.info(f"    FRCL form elements: {inputs[:15]}")

                    # Find and click the appropriate radio button
                    # Try multiple selector strategies
                    radio_clicked = False
                    for radio_sel in [
                        f"input[type=radio][value*='{search_type[:4]}']",
                        f"input[type=radio][id*='Sale']" if "Sale" in search_type else f"input[type=radio][id*='File']",
                        "input[type=radio]",
                    ]:
                        radios = page.locator(radio_sel)
                        count = await radios.count()
                        if count > 0:
                            if search_type == "Sale Date":
                                await radios.first.click()
                            else:
                                # Click second radio for File Date
                                idx = 1 if count > 1 else 0
                                await radios.nth(idx).click()
                            radio_clicked = True
                            await page.wait_for_timeout(500)
                            break

                    # Select year
                    year_sels = page.locator("select")
                    sel_count = await year_sels.count()
                    for si in range(sel_count):
                        sel = year_sels.nth(si)
                        sel_id = await sel.get_attribute("id") or ""
                        if "year" in sel_id.lower() or "yr" in sel_id.lower():
                            try:
                                await sel.select_option(str(year))
                                await page.wait_for_timeout(300)
                                break
                            except Exception:
                                pass
                    else:
                        # Try first select
                        if sel_count > 0:
                            try:
                                await year_sels.first.select_option(str(year))
                                await page.wait_for_timeout(300)
                            except Exception:
                                pass

                    # Select month
                    for si in range(sel_count):
                        sel = year_sels.nth(si)
                        sel_id = await sel.get_attribute("id") or ""
                        if "month" in sel_id.lower() or "mon" in sel_id.lower():
                            try:
                                await sel.select_option(month_names[month])
                                await page.wait_for_timeout(300)
                                break
                            except Exception:
                                # Try numeric value
                                try:
                                    await sel.select_option(str(month))
                                    await page.wait_for_timeout(300)
                                except Exception:
                                    pass
                            break
                    else:
                        # Try second select
                        if sel_count > 1:
                            try:
                                await year_sels.nth(1).select_option(month_names[month])
                                await page.wait_for_timeout(300)
                            except Exception:
                                pass

                    # Click search button
                    for btn_sel in [
                        "input[value='Search']",
                        "input[type=submit]",
                        "input[type=button][value*='Search']",
                        "button:has-text('Search')",
                    ]:
                        btn = page.locator(btn_sel)
                        if await btn.count() > 0:
                            await btn.first.click()
                            await page.wait_for_load_state("domcontentloaded", timeout=20000)
                            await page.wait_for_timeout(2500)
                            break

                    # Paginate - FRCL uses page number links in a table at top
                    # Debug showed: row 0 = "12345678910..." = page number links
                    # Each page shows 38 records; click each page number
                    html = await page.content()
                    month_rows = []
                    first_rows = self._parse_frcl_page(html, year, month, search_type)
                    month_rows.extend(first_rows)

                    # Find how many pages exist from pagination row
                    from bs4 import BeautifulSoup as _BS4
                    _soup = _BS4(html, "lxml")
                    page_links = []
                    for _a in _soup.find_all("a"):
                        txt = _a.get_text(strip=True)
                        if txt.isdigit() and int(txt) > 1:
                            page_links.append(int(txt))
                    max_page = max(page_links) if page_links else 1
                    log.info(f"      FRCL pages detected: {max_page}")

                    # Click each page number
                    for pg in range(2, min(max_page + 1, 25)):
                        try:
                            pg_link = page.locator(f"a:text-is('{pg}')")
                            if await pg_link.count() == 0:
                                # Try broader selector
                                pg_link = page.locator(f"a").filter(has_text=str(pg))
                            if await pg_link.count() > 0:
                                await pg_link.first.click()
                                await page.wait_for_load_state("domcontentloaded", timeout=15000)
                                await page.wait_for_timeout(1500)
                                pg_html = await page.content()
                                pg_rows = self._parse_frcl_page(pg_html, year, month, search_type)
                                month_rows.extend(pg_rows)
                                if len(pg_rows) == 0:
                                    break
                            else:
                                break
                        except Exception as pg_e:
                            log.warning(f"      FRCL page {pg} error: {pg_e}")
                            break

                    frcl_count += len(month_rows)
                    self.records.extend(month_rows)
                    log.info(f"    FRCL [{search_type}] {month_names[month]} {year}: {len(month_rows)} foreclosures ({max_page} pages)")

                except Exception as e:
                    log.warning(f"    FRCL [{search_type}] {year}-{month:02d} error: {e}")
                    continue

        log.info(f"  FRCL total: {frcl_count} foreclosure postings")

    def _parse_frcl_page(self, html: str, year: int, month: int, search_type: str = "Sale Date") -> list:
        """
        Parse FRCL results page.
        The page renders a table with columns like:
          Document ID | Grantor Name | Sale Date | Property Address | ...
        """
        from bs4 import BeautifulSoup
        import re as _re
        soup = BeautifulSoup(html, "lxml")
        rows_out = []
        seen = set()

        # Log table count and sizes for debug
        tables = soup.find_all("table")
        log.info(f"    FRCL parser: {len(tables)} tables on page")

        # Find the results table — look for one with multiple rows of data
        # and content that looks like document records
        best_table = None
        best_score = 0
        for tbl in tables:
            trs = tbl.find_all("tr")
            if len(trs) < 2:
                continue
            txt = tbl.get_text(" ")
            # Score by presence of date patterns and name-like content
            score = 0
            if _re.search(r'\d{1,2}/\d{1,2}/\d{4}', txt):
                score += 3
            if len(trs) > 5:
                score += len(trs)
            if "Grantor" in txt or "Trustee" in txt or "Mortgage" in txt:
                score += 2
            if score > best_score:
                best_score = score
                best_table = tbl

        if best_table is None:
            # Dump page text snippet for debugging
            page_txt = soup.get_text(" ")[:500]
            log.info(f"    FRCL no table found. Page snippet: {page_txt[:200]}")
            return rows_out

        log.info(f"    FRCL best table: {len(best_table.find_all('tr'))} rows, score={best_score}")

        # Log first few rows for debugging
        for ri, tr in enumerate(best_table.find_all("tr")[:5]):
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            log.info(f"    FRCL row {ri}: {cells[:6]}")

        # Parse all rows
        header_map = {}
        for ri, tr in enumerate(best_table.find_all("tr")):
            cells_raw = tr.find_all(["td", "th"])
            cells = [c.get_text(strip=True) for c in cells_raw]

            if not cells or not any(cells):
                continue

            # Detect header row
            if ri == 0 or any(h in " ".join(cells).upper() for h in
                              ["DOCUMENT", "GRANTOR", "SALE DATE", "FILE DATE", "ADDRESS", "INSTRUMENT"]):
                for ci, c in enumerate(cells):
                    header_map[ci] = c.upper().strip()
                continue

            if len(cells) < 2:
                continue

            # Try to extract fields using header map or positional guessing
            def get_field(*keys):
                for ci, hdr in header_map.items():
                    for k in keys:
                        if k in hdr and ci < len(cells):
                            return cells[ci]
                return ""

            # Confirmed FRCL layout from debug:
            # col 0: blank, col 1: Doc ID, col 2: Sale Date, col 3: File Date, col 4: Pgs
            # Grantor name not available in list view — must open individual document
            doc_num  = cells[1] if len(cells) > 1 else cells[0] if cells else ""
            sale_dt  = cells[2] if len(cells) > 2 else f"{year}-{month:02d}-01"
            file_dt  = cells[3] if len(cells) > 3 else ""
            grantor  = ""  # Not available in list view
            address  = ""  # Not available in list view — HCAD match will find it

            # Skip obvious non-data rows
            if not doc_num or len(doc_num) < 2:
                continue
            if doc_num.upper() in ["DOCUMENT ID", "DOC ID", "FILE NUMBER", ""]:
                continue
            # Deduplicate
            key = (doc_num, sale_dt)
            if key in seen:
                continue
            seen.add(key)

            # Normalize sale date
            filed_iso = ""
            for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
                try:
                    from datetime import datetime as _dt
                    filed_iso = _dt.strptime(sale_dt[:10], fmt).strftime("%Y-%m-%d")
                    break
                except Exception:
                    pass
            if not filed_iso:
                filed_iso = f"{year}-{month:02d}-01"

            rows_out.append({
                "doc_num":           doc_num,
                "doc_type":          "FRCL",
                "filed":             filed_iso,
                "cat":               "fc",
                "cat_label":         "Foreclosure Sale",
                "owner":             grantor,
                "grantee":           "",
                "contact":           grantor,
                "amount":            None,
                "legal":             address,
                "clerk_url":         f"https://www.cclerk.hctx.net/applications/websearch/FRCL_R.aspx",
                "frcl_sale_date":    filed_iso,
                "frcl_search_type":  search_type,
                "source":            "FRCL",
            })

        return rows_out

# ─── Enrich & Score ───────────────────────────────────────────────────────────
def enrich_records(raw: list, parcel: HCADParcelLookup) -> list:
    today    = datetime.now()
    seen     = set()
    enriched = []
    engine   = getattr(parcel, "engine", None)
    conf_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "NONE": 0}

    for rec in raw:
        try:
            key = (rec.get("doc_num", ""), rec.get("doc_type", ""))
            if key in seen:
                continue
            seen.add(key)

            if engine:
                rec = engine.enrich(rec)
            else:
                p = parcel.find(rec.get("owner", ""))
                rec["prop_address"] = p["site_addr"]  if p else ""
                rec["prop_city"]    = p["site_city"]  if p else "Houston"
                rec["prop_state"]   = "TX"
                rec["prop_zip"]     = p["site_zip"]   if p else ""
                rec["mail_address"] = p["mail_addr"]  if p else ""
                rec["mail_city"]    = p["mail_city"]  if p else ""
                rec["mail_state"]   = p["mail_state"] if p else "TX"
                rec["mail_zip"]     = p["mail_zip"]   if p else ""
                rec["match_confidence"] = "MEDIUM" if p else "NONE"
                rec["hcad_url"] = ""

            conf_counts[rec.get("match_confidence", "NONE")] += 1
            flags        = build_flags(rec, today)
            rec["flags"] = flags
            rec["score"] = compute_score(rec, flags)
            enriched.append(rec)
        except Exception as e:
            log.warning(f"Enrich error: {e}")

    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)
    with_addr = sum(1 for r in enriched if r.get("prop_address"))
    log.info(f"Enriched {len(enriched)} unique records | {with_addr} with address")
    log.info(f"  Confidence: HIGH={conf_counts['HIGH']} "
             f"MEDIUM={conf_counts['MEDIUM']} "
             f"LOW={conf_counts['LOW']} "
             f"NONE={conf_counts['NONE']}")
    return enriched

# ─── Save ─────────────────────────────────────────────────────────────────────
def save_records(records: list, today: datetime, days_back: int):
    payload = {
        "fetched_at":   today.isoformat(),
        "source":       "Harris County Clerk / HCAD",
        "date_range": {
            "from": (today - timedelta(days=days_back)).strftime("%Y-%m-%d"),
            "to":   today.strftime("%Y-%m-%d"),
        },
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":      records,
    }
    for d in OUTPUT_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        (d / "records.json").write_text(
            json.dumps(payload, indent=2, default=str))
        log.info(f"Saved → {d}/records.json")


def export_ghl_csv(records: list, today: datetime):
    cols = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]
    rows = []
    for r in records:
        try:
            parts  = (r.get("owner") or "").strip().split()
            amount = r.get("amount")
            rows.append({
                "First Name":             parts[0] if parts else "",
                "Last Name":              " ".join(parts[1:]) if len(parts) > 1 else "",
                "Mailing Address":        r.get("mail_address", ""),
                "Mailing City":           r.get("mail_city", ""),
                "Mailing State":          r.get("mail_state", "TX"),
                "Mailing Zip":            r.get("mail_zip", ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city", ""),
                "Property State":         r.get("prop_state", "TX"),
                "Property Zip":           r.get("prop_zip", ""),
                "Lead Type":              r.get("cat_label", ""),
                "Document Type":          r.get("doc_type", ""),
                "Date Filed":             r.get("filed", ""),
                "Document Number":        r.get("doc_num", ""),
                "Amount/Debt Owed":       f"${amount:,.2f}" if amount else "",
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":                 "Harris County Clerk",
                "Public Records URL":     r.get("clerk_url", ""),
            })
        except Exception:
            continue

    for d in OUTPUT_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"ghl_export_{today.strftime('%Y%m%d')}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
        log.info(f"GHL CSV → {path} ({len(rows)} rows)")


# ─── Main ─────────────────────────────────────────────────────────────────────
async def main():
    today     = datetime.now()
    days_back = int(os.environ.get("DAYS_BACK", "7"))

    log.info("=" * 60)
    log.info("Harris County Motivated Seller Scraper")
    log.info(f"Date range: last {days_back} days")
    log.info("=" * 60)

    log.info("\n[1/3] Loading HCAD parcel data...")
    parcel = HCADParcelLookup()
    try:
        parcel.load()
    except Exception as e:
        log.warning(f"HCAD load error: {e}")

    log.info("\n[2/3] Scraping Harris County Clerk...")
    raw = []
    try:
        raw = await HarrisClerkScraper(days_back=days_back).scrape_all()
    except Exception as e:
        log.error(f"Scraper error: {e}")

    log.info("\n[3/3] Enriching and scoring...")
    records = enrich_records(raw or [], parcel)

    log.info("\nSaving outputs...")
    save_records(records, today, days_back)
    export_ghl_csv(records, today)

    log.info(f"\n{'='*60}")
    log.info(f"✓ COMPLETE")
    log.info(f"  Total records:    {len(records)}")
    log.info(f"  With address:     {sum(1 for r in records if r.get('prop_address'))}")
    log.info(f"  Hot leads (≥70):  {sum(1 for r in records if r.get('score',0) >= 70)}")
    log.info(f"  Warm leads (≥50): {sum(1 for r in records if 50 <= r.get('score',0) < 70)}")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
