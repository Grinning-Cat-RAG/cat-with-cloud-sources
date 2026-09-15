"""Minimal AWS Signature Version 4 for GET requests to S3 (no botocore dependency)."""
from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timezone
from typing import Dict, Mapping
from urllib.parse import quote, urlsplit

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _encode(value: str) -> str:
    return quote(value, safe="-_.~")


def sign_get(
    *,
    url: str,
    params: Mapping[str, str],
    region: str,
    access_key_id: str,
    secret_access_key: str,
    session_token: str | None = None,
    service: str = "s3",
    now: datetime | None = None,
) -> Dict[str, str]:
    """Return the headers (Authorization, x-amz-*) that sign a GET with an empty body.

    ``url`` must not contain a query string: pass it in ``params``. The path must be
    already URI-encoded as it will be sent (S3 does not double-encode the path).
    """
    parts = urlsplit(url)
    if parts.query:
        raise ValueError("Pass query parameters via params")
    now = now or datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    host = parts.hostname or ""
    if parts.port and parts.port not in (80, 443):
        host = f"{host}:{parts.port}"

    headers = {"host": host, "x-amz-content-sha256": EMPTY_SHA256, "x-amz-date": amz_date}
    if session_token:
        headers["x-amz-security-token"] = session_token

    canonical_query = "&".join(
        f"{_encode(k)}={_encode(v)}" for k, v in sorted((str(k), str(v)) for k, v in params.items())
    )
    signed_headers = ";".join(sorted(headers))
    canonical_headers = "".join(f"{k}:{headers[k].strip()}\n" for k in sorted(headers))
    canonical_request = "\n".join([
        "GET", parts.path or "/", canonical_query, canonical_headers, signed_headers, EMPTY_SHA256,
    ])

    scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])
    key = _hmac(("AWS4" + secret_access_key).encode("utf-8"), date_stamp)
    for part in (region, service, "aws4_request"):
        key = _hmac(key, part)
    signature = hmac.new(key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    signed = {k: v for k, v in headers.items() if k != "host"}
    signed["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key_id}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return signed
