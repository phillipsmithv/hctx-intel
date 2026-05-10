"""
Harris County Address Enrichment Engine
Multi-strategy lookup with confidence scoring.

Priority:
1. Legal description match (subdivision + lot + block) -> HIGH
2. Doc-type-aware name match -> MEDIUM/LOW
3. No match -> HCAD lookup URL for manual verification
"""

import re
import logging
import urllib.parse
from typing import Optional

log = logging.getLogger(__name__)


def parse_legal_description(legal):
    result = {"subdivision": "", "section": "", "lot": "", "block": "", "abstract": "", "tract": ""}
    if not legal:
        return result
    legal = legal.upper().strip()
    m = re.search(r'DESC[\s:]+(.+?)(?:\s+SEC[\s:]|\s+LOT[\s:]|\s+BLK[\s:]|\s+BLOCK[\s:]|\s+TR[\s:]|\s+ABST[\s:]|$)', legal)
    if m:
        result["subdivision"] = m.group(1).strip()
    for key, pat in [
        ("section",  r'SEC(?:TION)?[\s:]+(\w+)'),
        ("lot",      r'LOT[\s:]+(\w+)'),
        ("block",    r'(?:BLK|BLOCK)[\s:]+(\w+)'),
        ("abstract", r'ABST(?:RACT)?[\s:]+(\w+)'),
        ("tract",    r'(?:\bTR\b|TRACT)[\s:]+(\w+)'),
    ]:
        m2 = re.search(pat, legal)
        if m2:
            result[key] = m2.group(1).strip()
    return result


def legal_match_key(parsed):
    sub = parsed.get("subdivision", "").strip()
    sec = parsed.get("section", "").strip()
    lot = parsed.get("lot", "").strip()
    blk = parsed.get("block", "").strip()
    if sub and lot and blk:
        return f"{sub}|{sec}|{lot}|{blk}"
    if sub and lot:
        return f"{sub}||{lot}|"
    return None


_STRIP_WORDS = [
    "INCORPORATED", "CORPORATION", "ASSOCIATION", "COMMUNITY",
    "HOMEOWNERS", "COMMITTEE", "FOUNDATION",
    "TRUSTEE", "LIMITED", "OWNERS", "ASSOC", "ASSN",
    "TRUST", "CORP", "INC", "LLC", "LTD", "LP",
    "ETAL", "ET AL", "ET UX", "JR", "SR",
]
_STRIP_RE = re.compile(
    r'\b(' + '|'.join(re.escape(w) for w in _STRIP_WORDS) + r')\b',
    re.IGNORECASE
)


def normalize_name(name):
    if not name:
        return ""
    n = name.upper().strip()
    # Remove apostrophes before stripping (fixes HOMEOWNER'S -> HOMEOWNER)
    n = re.sub(r"\'S\b", "", n)
    n = re.sub(r"[',.]", " ", n)
    n = _STRIP_RE.sub(" ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def name_variants(name):
    n = normalize_name(name)
    if not n:
        return []
    parts = n.split()
    variants = {n}
    if len(parts) >= 2:
        # Last-first swap
        variants.add(f"{parts[-1]} {' '.join(parts[:-1])}")
        variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}")
    # For entities: also try first 2 and 3 word prefixes
    # e.g. "GRAND OAKS" matches "GRAND OAKS HOMEOWNER..."
    if len(parts) >= 3:
        variants.add(' '.join(parts[:2]))
        variants.add(' '.join(parts[:3]))
    return [v for v in variants if len(v) > 3]


def get_match_targets(rec):
    cat     = rec.get("cat", "")
    grantor = (rec.get("owner") or "").strip()
    grantee = (rec.get("grantee") or "").strip()
    contact = (rec.get("contact") or "").strip()
    
    # contact field = pre-selected best match target based on doc type
    # LP/FC: contact = grantee (homeowner being sued/foreclosed)
    # Others: contact = grantor (the debtor)
    if contact:
        others = [t for t in [grantee, grantor] if t and t != contact]
        return [contact] + others
    if cat in ("lp", "fc"):
        return [t for t in [grantee, grantor] if t]
    return [t for t in [grantor, grantee] if t]


class EnrichmentEngine:
    def __init__(self):
        self.legal_lookup = {}
        self.name_lookup  = {}
        self.acct_lookup  = {}

    def _add(self, lookup, key, parcel):
        if not key:
            return
        if key not in lookup:
            lookup[key] = []
        lookup[key].append(parcel)

    def build_from_hcad(self, owners_records, acct_map, parcel_tieback=None):
        log.info("Building enrichment indexes...")
        # First pass: build acct→parcel and legal index
        for acct, parcel in acct_map.items():
            self.acct_lookup[acct] = parcel
            if parcel_tieback and acct in parcel_tieback:
                pt  = parcel_tieback[acct]
                key = legal_match_key(pt)
                if key:
                    self._add(self.legal_lookup, key, parcel)

        # Second pass: build name index from owners
        for rec in owners_records:
            try:
                acct = (rec.get("acct") or "").strip()
                name = (rec.get("name") or "").strip()
                aka  = (rec.get("aka")  or "").strip()
                if not acct or not name:
                    continue
                parcel = acct_map.get(acct)
                if not parcel:
                    continue
                # Store owner norm on parcel for legal+name tie-breaking
                if not parcel.get("_owner_norm"):
                    parcel["_owner_norm"] = normalize_name(name)
                for raw in [name, aka]:
                    if not raw:
                        continue
                    for v in name_variants(raw):
                        self._add(self.name_lookup, v, parcel)
                    # Also index first 2-3 words of normalized name
                    # so "GRAND OAKS" matches "GRAND OAKS HOMEOWNERS ASSOC"
                    norm = normalize_name(raw)
                    norm_parts = norm.split()
                    for n_words in [2, 3]:
                        if len(norm_parts) >= n_words:
                            prefix = ' '.join(norm_parts[:n_words])
                            if len(prefix) > 4:
                                self._add(self.name_lookup, prefix, parcel)
            except Exception:
                continue
        log.info(f"  Name index:  {len(self.name_lookup):,} keys")
        log.info(f"  Legal index: {len(self.legal_lookup):,} keys")
        log.info(f"  Acct index:  {len(self.acct_lookup):,} keys")

    def enrich(self, rec):
        legal_raw   = rec.get("legal", "")
        search_name = (rec.get("grantee") if rec.get("cat") in ("lp", "fc")
                       else rec.get("owner") or "")
        rec["hcad_url"] = self._hcad_url(legal_raw, search_name)

        # Strategy 1: Legal description match
        parsed    = parse_legal_description(legal_raw)
        legal_key = legal_match_key(parsed)
        if legal_key and legal_key in self.legal_lookup:
            matches = self.legal_lookup[legal_key]
            if len(matches) == 1:
                return self._apply(rec, matches[0], "HIGH", "Legal description — unique parcel")
            # Try to narrow by name
            for target in get_match_targets(rec):
                norm = normalize_name(target)
                for m in matches:
                    if norm and norm == m.get("_owner_norm", ""):
                        return self._apply(rec, m, "HIGH", "Legal + name — confirmed")
            return self._apply(rec, matches[0], "MEDIUM",
                               f"Legal description — {len(matches)} parcels, took first")

        # Strategy 2: Name match
        for target in get_match_targets(rec):
            for v in name_variants(target):
                if v not in self.name_lookup:
                    continue
                matches = self.name_lookup[v]
                v_words = len(v.split())
                if len(matches) == 1:
                    conf   = "MEDIUM" if v_words >= 3 else "LOW"
                    reason = f"Name match ({v_words}w) — unique: {v}"
                    return self._apply(rec, matches[0], conf, reason)
                else:
                    reason = f"Name match — {len(matches)} duplicates: {v}"
                    return self._apply(rec, matches[0], "LOW", reason)

        # Strategy 3: Subdivision name from legal description
        parsed = parse_legal_description(legal_raw)
        sub    = parsed.get("subdivision", "").strip()
        if sub and len(sub) > 5 and sub in self.name_lookup:
            matches = self.name_lookup[sub]
            if len(matches) == 1:
                return self._apply(rec, matches[0], "LOW", f"Subdivision match: {sub}")

        # No match
        rec.update({
            "match_confidence": "NONE", "match_reason": "No match found",
            "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
            "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
        })
        return rec

    def _apply(self, rec, parcel, confidence, reason):
        rec["match_confidence"] = confidence
        rec["match_reason"]     = reason
        rec["prop_address"]     = parcel.get("site_addr", "")
        rec["prop_city"]        = parcel.get("site_city", "Houston")
        rec["prop_state"]       = parcel.get("site_state", "TX")
        rec["prop_zip"]         = parcel.get("site_zip", "")
        rec["mail_address"]     = parcel.get("mail_addr", "")
        rec["mail_city"]        = parcel.get("mail_city", "")
        rec["mail_state"]       = parcel.get("mail_state", "TX")
        rec["mail_zip"]         = parcel.get("mail_zip", "")
        if not rec["mail_address"] and rec["prop_address"]:
            rec["mail_address"] = rec["prop_address"]
            rec["mail_city"]    = rec["prop_city"]
            rec["mail_state"]   = rec["prop_state"]
            rec["mail_zip"]     = rec["prop_zip"]
        return rec

    def _hcad_url(self, legal, name):
        parsed = parse_legal_description(legal)
        sub = parsed.get("subdivision", "")
        lot = parsed.get("lot", "")
        blk = parsed.get("block", "")
        if sub and lot:
            params = urllib.parse.urlencode({"s": sub, "l": lot, "b": blk})
            return f"https://hcad.org/property-search/real-property/?{params}"
        if name:
            params = urllib.parse.urlencode({"name": str(name)[:60]})
            return f"https://hcad.org/property-search/real-property/search-by-owner-name/?{params}"
        return "https://hcad.org/property-search/real-property/"
