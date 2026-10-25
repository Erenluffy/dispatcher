"""Worker affinity for stateful IDs.

Waveform files and share snapshots live on the worker that created them.  The
dispatcher prefixes those IDs with that worker's five-character tag, strips the
tag before forwarding, and routes later reads back to the owner.

Worker-generated IDs are fixed formats:
  * waveform UUID: 36 chars, e.g. 3f2b...-...
  * share id:      10 hex chars, e.g. 9c1a2b3d4e

The prefixed forms are 41 and 15 hex-ish characters respectively, both accepted
by the frontend's existing ``[a-f0-9]+`` share regex and by the backend's
``^[a-f0-9]{6,32}$`` validator once the tag has been stripped.
"""

from __future__ import annotations

import json
import re
from urllib.parse import urlsplit, urlunsplit

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_SHARE_RE = re.compile(r"^[a-f0-9]{6,32}$")
_TAG_RE = re.compile(r"^[0-9a-f]{5}$")
_WAVEFORM_PATH_RE = re.compile(r"(/api/waveform/)([^/?#]+)")
_SHARE_REF_RE = re.compile(r"(#share=)([a-f0-9]{6,32})")

WAVEFORM_ID_FIELDS = ("waveform_id",)
WAVEFORM_URL_FIELDS = ("waveform_url", "waveform_download_url")


def is_uuid(value: object) -> bool:
    return isinstance(value, str) and bool(_UUID_RE.match(value))


def is_share_id(value: object) -> bool:
    return isinstance(value, str) and bool(_SHARE_RE.match(value))


def encode_waveform_id(tag: str, raw: str) -> str:
    if not is_uuid(raw):
        raise ValueError("waveform id is not a UUID")
    return f"{tag}{raw}"


def decode_waveform_id(value: str) -> tuple[str | None, str] | None:
    """Return ``(tag, raw)`` or ``None`` when the path segment is not an ID.

    A 41-character value that ends in a UUID but carries an unknown five-hex
    tag is returned with that unknown tag; the caller responds 404 rather than
    silently treating it as a legacy raw UUID.
    """
    if len(value) == 41 and _TAG_RE.match(value[:5]) and is_uuid(value[5:]):
        return value[:5], value[5:]
    if is_uuid(value):
        return None, value
    return None


def decode_share_id(value: str) -> tuple[str | None, str] | None:
    if (len(value) == 15 and _TAG_RE.match(value[:5])
            and _SHARE_RE.match(value[5:])):
        return value[:5], value[5:]
    if is_share_id(value):
        return None, value
    return None


def _tag_waveform_ref(value: str, tag: str) -> str:
    match = _WAVEFORM_PATH_RE.search(value)
    if not match:
        return value
    raw = match.group(2)
    if not is_uuid(raw):
        return value
    rewritten = (value[:match.start(2)] + tag + raw
                 + value[match.end(2):])
    # Worker-generated URLs are relative today.  If a future backend returns an
    # absolute worker URL, make it dispatcher-relative so the tagged ID is sent
    # back through this service instead of directly to the worker.
    if rewritten.startswith(("http://", "https://")):
        parsed = urlsplit(rewritten)
        rewritten = urlunsplit(("", "", parsed.path, parsed.query,
                                parsed.fragment))
    return rewritten


def retag_compute_payload(payload: dict, tag: str) -> bool:
    """Tag waveform fields in a run/submit response in place.

    Returns True when the payload changed, so callers can avoid re-serialising
    unchanged JSON (and therefore avoid altering an error response's bytes).
    """
    changed = False
    for field in WAVEFORM_ID_FIELDS:
        value = payload.get(field)
        if is_uuid(value):
            payload[field] = f"{tag}{value}"
            changed = True
    for field in WAVEFORM_URL_FIELDS:
        value = payload.get(field)
        if isinstance(value, str):
            rewritten = _tag_waveform_ref(value, tag)
            if rewritten != value:
                payload[field] = rewritten
                changed = True
    return changed


def retag_share_payload(payload: dict, tag: str) -> bool:
    changed = False
    share_id = payload.get("id")
    if is_share_id(share_id):
        payload["id"] = f"{tag}{share_id}"
        changed = True
    url = payload.get("url")
    if isinstance(url, str):
        def replace_ref(match: re.Match) -> str:
            return match.group(1) + tag + match.group(2)

        rewritten = _SHARE_REF_RE.sub(replace_ref, url)
        if rewritten != url:
            payload["url"] = rewritten
            changed = True
    return changed


def rewrite_waveform_html(text: str, raw_id: str, tagged_id: str) -> str:
    return text.replace(raw_id, tagged_id)


def dumps_compact(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")
