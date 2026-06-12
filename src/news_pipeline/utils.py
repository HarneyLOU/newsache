from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


TRACKING_QUERY_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
}


def stable_hash(*parts: str | None) -> str:
    raw = "||".join((part or "").strip() for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def normalize_url(url: str) -> str:
    split = urlsplit(url.strip())

    scheme = split.scheme.lower() or "https"
    netloc = split.netloc.lower()

    path = split.path.rstrip("/") or "/"

    query_pairs = parse_qsl(split.query, keep_blank_values=False)
    filtered_query_pairs = [
        (key, value)
        for key, value in query_pairs
        if key.lower() not in TRACKING_QUERY_PARAMS
    ]

    query = urlencode(sorted(filtered_query_pairs))

    return urlunsplit((scheme, netloc, path, query, ""))