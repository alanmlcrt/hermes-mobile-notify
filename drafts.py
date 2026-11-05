"""Brouillons de mail et de message : stockage, outils de l'agent, décision d'envoi.

Repris et adapté de hermes-draft-cards (arsiktech, licence MIT, voir
LICENSE-draft-cards), commit 86ba497 : carte rendue par l'app Hermes Mobile, et
``approved`` réservé au bouton Envoyer pour que le hook de mail puisse s'y fier.
Bibliothèque standard seulement : les outils et les routes partagent ce fichier.
"""

from __future__ import annotations

import json
import logging
import re
import secrets
import sqlite3
import time
from contextlib import closing
from typing import Any, Callable, Iterable, Optional

PLUGIN_NAME = "hermes-mobile-notify"

KINDS = ("email", "message")
STATUSES = ("draft", "approved", "sent", "saved", "failed", "discarded")
# Statuses the agent may report. Never "approved": only the app's Envoyer button sets
# it, which is what lets the mail hook trust it.
AGENT_STATUSES = ("sent", "saved", "failed", "discarded", "draft")
USER_STATUSES = ("draft", "discarded")

FIELD_LIMITS = {"subject": 300, "body": 20_000, "from": 320, "platform": 40, "note": 280}
MAX_RECIPIENTS = 50
MAX_RECIPIENT_LEN = 320
MAX_VERSIONS = 60

_ID_RE = re.compile(r"^d_[0-9a-f]{8}$")
_ADDRESS = re.compile(r"[^\s<>,;\"]+@[^\s<>,;\"]+")
_SCHEMA = """
CREATE TABLE IF NOT EXISTS drafts (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    platform TEXT NOT NULL DEFAULT '',
    session_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'draft',
    status_detail TEXT NOT NULL DEFAULT '',
    status_at REAL NOT NULL DEFAULT 0,
    approved_version INTEGER,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS versions (
    draft_id TEXT NOT NULL REFERENCES drafts(id) ON DELETE CASCADE,
    v INTEGER NOT NULL,
    card INTEGER NOT NULL,
    author TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    fields TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (draft_id, v)
);
"""


class DraftError(ValueError):
    """A caller-facing problem (bad input, unknown id, wrong state). ``code`` maps to HTTP."""

    def __init__(self, message: str, code: int = 400):
        super().__init__(message)
        self.code = code


def _default_connect() -> sqlite3.Connection:
    from plugins.plugin_storage import plugin_db

    return plugin_db(PLUGIN_NAME, "drafts.db")


class DraftStore:
    """Thin, synchronous repository. Open a store per request; connections are short-lived."""

    def __init__(self, connect: Optional[Callable[[], sqlite3.Connection]] = None, clock: Callable[[], float] = time.time):
        self._connect = connect or _default_connect
        self._clock = clock

    # ------------------------------------------------------------------ helpers
    def _db(self) -> sqlite3.Connection:
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        conn.executescript(_SCHEMA)
        return conn

    @staticmethod
    def valid_id(draft_id: Any) -> bool:
        return isinstance(draft_id, str) and bool(_ID_RE.fullmatch(draft_id))

    def _require(self, conn: sqlite3.Connection, draft_id: Any) -> sqlite3.Row:
        if not self.valid_id(draft_id):
            raise DraftError("draft_id must look like d_1a2b3c4d", 400)
        row = conn.execute("SELECT * FROM drafts WHERE id = ?", (draft_id,)).fetchone()
        if row is None:
            raise DraftError(f"No draft {draft_id}. It may belong to another profile.", 404)
        return row

    @staticmethod
    def _versions(conn: sqlite3.Connection, draft_id: str) -> list[dict]:
        rows = conn.execute(
            "SELECT v, card, author, note, fields, created_at FROM versions WHERE draft_id = ? ORDER BY v", (draft_id,)
        ).fetchall()
        return [
            {
                "v": r["v"],
                "card": r["card"],
                "author": r["author"],
                "note": r["note"],
                "fields": json.loads(r["fields"]),
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    def _record(self, conn: sqlite3.Connection, draft_id: str) -> dict:
        row = self._require(conn, draft_id)
        versions = self._versions(conn, draft_id)
        return {
            "id": row["id"],
            "kind": row["kind"],
            "platform": row["platform"],
            "session_id": row["session_id"],
            "status": row["status"],
            "status_detail": row["status_detail"],
            "status_at": row["status_at"],
            "approved_version": row["approved_version"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "latest_version": versions[-1]["v"] if versions else 0,
            "latest_card": max((v["card"] for v in versions), default=0),
            "versions": versions,
        }

    # ------------------------------------------------------------------ public API
    def get(self, draft_id: str) -> dict:
        with closing(self._db()) as conn:
            return self._record(conn, draft_id)

    def create(self, *, kind: str, fields: dict, platform: str = "", session_id: str = "", note: str = "") -> dict:
        kind = kind if kind in KINDS else "email"
        clean = normalize_fields(fields, kind)
        if not clean["body"].strip():
            raise DraftError("body is required")
        now = self._clock()
        with closing(self._db()) as conn, conn:
            draft_id = self._new_id(conn)
            conn.execute(
                "INSERT INTO drafts (id, kind, platform, session_id, status, created_at, updated_at, status_at)"
                " VALUES (?, ?, ?, ?, 'draft', ?, ?, ?)",
                (draft_id, kind, _clip(platform, "platform"), _clip(session_id, "from"), now, now, now),
            )
            conn.execute(
                "INSERT INTO versions (draft_id, v, card, author, note, fields, created_at) VALUES (?, 1, 1, 'agent', ?, ?, ?)",
                (draft_id, _clip(note, "note"), json.dumps(clean, ensure_ascii=False), now),
            )
            return self._record(conn, draft_id)

    def revise(self, draft_id: str, *, fields: dict, note: str = "", author: str = "agent",
               card: Optional[int] = None, base_version: Optional[int] = None,
               platform: Optional[str] = None) -> dict:
        """Append a version. Omitted fields carry over from the latest version.

        ``author='agent'`` opens a new card (card = previous max card + 1).
        ``author='user'`` keeps the card the user edited in (``card`` is required) and
        must name the ``base_version`` it edited, so a stale card can't overwrite a
        newer agent revision.
        """
        now = self._clock()
        with closing(self._db()) as conn, conn:
            row = self._require(conn, draft_id)
            if row["status"] in ("sent", "saved") and author == "user":
                raise DraftError("This draft was already sent or saved; it can't be edited.", 409)
            versions = self._versions(conn, draft_id)
            if len(versions) >= MAX_VERSIONS:
                raise DraftError(f"A draft keeps at most {MAX_VERSIONS} versions. Start a new draft.", 409)
            latest = versions[-1]
            if author == "user":
                if base_version is None or int(base_version) != latest["v"]:
                    raise DraftError("Hermes revised this draft while you were editing. Your copy is out of date.", 409)
                if card is None or not any(v["card"] == int(card) for v in versions):
                    raise DraftError("Unknown card for this draft.", 400)
                target_card = int(card)
            else:
                author = "agent"
                target_card = max(v["card"] for v in versions) + 1
            merged = {**latest["fields"], **{k: v for k, v in (fields or {}).items() if v is not None}}
            clean = normalize_fields(merged, row["kind"])
            if not clean["body"].strip():
                raise DraftError("body can't be empty")
            if clean == latest["fields"]:
                if author == "user":
                    return self._record(conn, draft_id)  # nothing changed: no new version, no error
                raise DraftError("That revision is identical to the current version.", 409)
            new_v = latest["v"] + 1
            conn.execute(
                "INSERT INTO versions (draft_id, v, card, author, note, fields, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (draft_id, new_v, target_card, author, _clip(note, "note"), json.dumps(clean, ensure_ascii=False), now),
            )
            # Any edit re-opens the draft for approval: an approval was for the old text.
            sets = ["updated_at = ?", "status = 'draft'", "approved_version = NULL", "status_detail = ''", "status_at = ?"]
            params: list[Any] = [now, now]
            if platform is not None:
                sets.append("platform = ?")
                params.append(_clip(platform, "platform"))
            conn.execute(f"UPDATE drafts SET {', '.join(sets)} WHERE id = ?", (*params, draft_id))
            return self._record(conn, draft_id)

    def approve(self, draft_id: str, version: int) -> dict:
        now = self._clock()
        with closing(self._db()) as conn, conn:
            row = self._require(conn, draft_id)
            versions = self._versions(conn, draft_id)
            if int(version) != versions[-1]["v"]:
                raise DraftError("Only the latest version can be approved.", 409)
            if row["status"] not in ("draft", "failed", "approved"):
                raise DraftError(f"This draft is already {row['status']}.", 409)
            conn.execute(
                "UPDATE drafts SET status = 'approved', approved_version = ?, status_detail = '', status_at = ?, updated_at = ? WHERE id = ?",
                (int(version), now, now, draft_id),
            )
            return self._record(conn, draft_id)

    def set_status(self, draft_id: str, status: str, detail: str = "", *, by: str = "agent") -> dict:
        allowed = AGENT_STATUSES if by == "agent" else USER_STATUSES
        if status not in allowed:
            raise DraftError(f"status must be one of: {', '.join(allowed)}")
        now = self._clock()
        with closing(self._db()) as conn, conn:
            row = self._require(conn, draft_id)
            current = row["status"]
            if by == "user" and current in ("sent", "saved"):
                raise DraftError(f"This draft is already {current}.", 409)
            approved = row["approved_version"]
            if status in ("draft", "discarded"):
                approved = None
            conn.execute(
                "UPDATE drafts SET status = ?, status_detail = ?, status_at = ?, updated_at = ?, approved_version = ? WHERE id = ?",
                (status, _clip(detail, "note"), now, now, approved, draft_id),
            )
            return self._record(conn, draft_id)

    def take_approved(self, mail: dict, max_age: float = 900) -> bool:
        """Consume the recent approval matching ``mail``: it covers a single send."""
        now = self._clock()
        with closing(self._db()) as conn, conn:
            rows = conn.execute(
                "SELECT d.id, v.fields FROM drafts d JOIN versions v "
                "ON v.draft_id = d.id AND v.v = d.approved_version "
                "WHERE d.status = 'approved' AND d.status_at > ?", (now - max_age,),
            ).fetchall()
            for row in rows:
                if same_mail(json.loads(row["fields"]), mail):
                    conn.execute("UPDATE drafts SET approved_version = NULL WHERE id = ?", (row["id"],))
                    return True
        return False

    def prune(self, keep: int = 500) -> int:
        with closing(self._db()) as conn, conn:
            old = [r["id"] for r in conn.execute("SELECT id FROM drafts ORDER BY updated_at DESC LIMIT -1 OFFSET ?", (keep,))]
            for draft_id in old:
                conn.execute("DELETE FROM versions WHERE draft_id = ?", (draft_id,))
                conn.execute("DELETE FROM drafts WHERE id = ?", (draft_id,))
            return len(old)

    @staticmethod
    def _new_id(conn: sqlite3.Connection) -> str:
        for _ in range(8):
            candidate = f"d_{secrets.token_hex(4)}"
            if conn.execute("SELECT 1 FROM drafts WHERE id = ?", (candidate,)).fetchone() is None:
                return candidate
        raise DraftError("Could not allocate a draft id", 500)


# ---------------------------------------------------------------------- normalisation
def _clip(value: Any, field: str) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return text[: FIELD_LIMITS.get(field, 320)]


def split_recipients(value: Any) -> list[str]:
    """Accept a list or a comma/semicolon/newline separated string; keep quoted display names intact."""
    if value is None:
        return []
    items: Iterable[Any]
    if isinstance(value, (list, tuple)):
        items = value
    else:
        items = _split_outside_quotes(str(value))
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = " ".join(str(item).split()).strip().strip(",;")
        if not text:
            continue
        text = text[:MAX_RECIPIENT_LEN]
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= MAX_RECIPIENTS:
            break
    return out


def _split_outside_quotes(text: str) -> list[str]:
    parts, buf, quoted, angle = [], [], False, False
    for ch in text:
        if ch == '"':
            quoted = not quoted
        elif ch == "<":
            angle = True
        elif ch == ">":
            angle = False
        if ch in ",;\n" and not quoted and not angle:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    parts.append("".join(buf))
    return parts


def normalize_fields(fields: dict, kind: str) -> dict:
    fields = fields or {}
    clean = {
        "from": _clip(fields.get("from", ""), "from").strip(),
        "to": split_recipients(fields.get("to")),
        "cc": split_recipients(fields.get("cc")) if kind == "email" else [],
        "bcc": split_recipients(fields.get("bcc")) if kind == "email" else [],
        "subject": " ".join(_clip(fields.get("subject", ""), "subject").split()) if kind == "email" else "",
        "body": _clip(fields.get("body", ""), "body").strip("\n"),
    }
    return clean


def _addresses(value: Any) -> list[str]:
    text = value if isinstance(value, str) else ",".join(value or [])
    return sorted(a.lower() for a in _ADDRESS.findall(text))


def _text(value: Any) -> str:
    return (value or "").replace("\r\n", "\n").strip()


def same_mail(fields: dict, mail: dict) -> bool:
    """A reply's recipients come from its thread, so only its body can be compared."""
    if _text(fields.get("body")) != _text(mail.get("body")) or fields.get("bcc"):
        return False
    return mail.get("kind") == "reply" or (
        _addresses(fields.get("to")) == _addresses(mail.get("to"))
        and _addresses(fields.get("cc")) == _addresses(mail.get("cc"))
        and " ".join(fields.get("subject", "").split()) == " ".join(mail.get("subject", "").split())
    )


def directive(draft_id: str, card: int) -> str:
    return f'::draft-card{{id="{draft_id}" v="{int(card)}"}}'


def plain_text(record: dict, version: Optional[dict] = None) -> str:
    """The draft as readable text for terminals and messaging surfaces."""
    version = version or record["versions"][-1]
    f = version["fields"]
    label = record.get("platform") or ("Email" if record["kind"] == "email" else "Message")
    lines = [f"{label} draft (v{version['v']})"]
    if f.get("subject"):
        lines.append(f"Subject: {f['subject']}")
    if f.get("from"):
        lines.append(f"From: {f['from']}")
    if f.get("to"):
        lines.append(f"To: {', '.join(f['to'])}")
    if f.get("cc"):
        lines.append(f"Cc: {', '.join(f['cc'])}")
    if f.get("bcc"):
        lines.append(f"Bcc: {', '.join(f['bcc'])}")
    lines.append("")
    lines.append(f.get("body", ""))
    return "\n".join(lines)


# ---------------------------------------------------------------------- agent tools
logger = logging.getLogger(__name__)

_RECIPIENTS = {
    "anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "string"}],
    "description": "Recipients: a list, or one comma-separated string. Use 'Name <address>' when you know the name.",
}

DRAFT_CARD = {
    "name": "draft_card",
    "description": (
        "Show an email or chat message you drafted for the user as a reviewable draft card, and keep "
        "every revision. Use it whenever the user asks you to write, draft, reply to or revise an email "
        "or message, BEFORE anything is sent.\n"
        "- New draft: omit draft_id. Revision: pass the same draft_id plus only the fields that change, "
        "and a short change_note. Each revision opens a new card in which the changed words are highlighted.\n"
        "- Then show it. In the Hermes Mobile app, put the returned `card` line alone on its own line in "
        "your reply and do NOT repeat the draft text. On every other surface (terminal, Telegram, Slack, …) "
        "show `text_fallback` instead.\n"
        "- This tool never sends anything. When the user presses Envoyer on the card (or clearly approves "
        "in chat), call draft_card_read, send exactly that version with whatever email or messaging tool "
        "you have, then report the outcome with draft_card_status."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "draft_id": {"type": "string", "description": "Omit for a new draft. Pass it to revise an existing draft."},
            "kind": {"type": "string", "enum": ["email", "message"], "description": "email (default) or a chat message (Slack, Telegram, iMessage, X DM…)."},
            "platform": {"type": "string", "description": "Optional label shown on the card, e.g. 'Gmail', 'Apple Mail', 'Slack', 'Telegram'."},
            "from": {"type": "string", "description": "Sender address or account, if known."},
            "to": _RECIPIENTS,
            "cc": _RECIPIENTS,
            "bcc": _RECIPIENTS,
            "subject": {"type": "string", "description": "Email subject. Ignored for messages."},
            "body": {"type": "string", "description": "The full message text in plain text, with blank lines between paragraphs. Required for a new draft."},
            "change_note": {"type": "string", "description": "Revision only: a few words on what changed, e.g. 'shorter, warmer close'."},
        },
        "required": [],
    },
}

DRAFT_CARD_READ = {
    "name": "draft_card_read",
    "description": (
        "Read a draft card's current state: the latest version (the user may have edited it in the card) "
        "and its status. Call it right before sending or saving a draft. Send only when status is 'approved' "
        "(the user pressed Envoyer) or the user clearly approved it in chat, and send the latest version "
        "word for word."
    ),
    "parameters": {
        "type": "object",
        "properties": {"draft_id": {"type": "string", "description": "The draft id, e.g. d_1a2b3c4d."}},
        "required": ["draft_id"],
    },
}

DRAFT_CARD_STATUS = {
    "name": "draft_card_status",
    "description": (
        "Report what happened to a draft so its card updates: 'sent' after you sent it, 'saved' after you "
        "saved it to the user's drafts folder, 'failed' if sending failed (put the reason in detail), "
        "'discarded' if the user dropped it. detail is a short human line such as 'via Gmail' or "
        "'No email account connected'."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "draft_id": {"type": "string"},
            "status": {"type": "string", "enum": ["sent", "saved", "failed", "discarded"]},
            "detail": {"type": "string", "description": "Short outcome line shown on the card."},
        },
        "required": ["draft_id", "status"],
    },
}


def _ok(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _err(message: str, **extra: Any) -> str:
    return json.dumps({"error": message, **extra}, ensure_ascii=False)


def _summary(record: dict) -> dict:
    latest = record["versions"][-1]
    return {
        "draft_id": record["id"],
        "version": latest["v"],
        "status": record["status"],
        "approved_version": record["approved_version"],
        "kind": record["kind"],
        "platform": record["platform"],
        "fields": latest["fields"],
        "edited_by_user": latest["author"] == "user",
    }


def make_handlers(store_factory=DraftStore):
    def draft_card(args: dict, session_id: str = "", **_: Any) -> str:
        try:
            store = store_factory()
            fields = {k: args.get(k) for k in ("from", "to", "cc", "bcc", "subject", "body")}
            draft_id = (args.get("draft_id") or "").strip()
            if draft_id:
                record = store.revise(
                    draft_id,
                    fields=fields,
                    note=args.get("change_note") or "",
                    platform=args.get("platform"),
                )
            else:
                record = store.create(
                    kind=args.get("kind") or "email",
                    fields=fields,
                    platform=args.get("platform") or "",
                    session_id=str(session_id or ""),
                    note=args.get("change_note") or "",
                )
                store.prune()
            latest = record["versions"][-1]
            return _ok({
                **_summary(record),
                "card": directive(record["id"], latest["card"]),
                "text_fallback": plain_text(record, latest),
                "next": (
                    "Hermes Mobile: reply with a one-line intro, then the `card` line alone on its own line. "
                    "Don't paste the draft text, the card shows it. Other surfaces: show text_fallback. "
                    "Wait for the user's approval before sending."
                ),
            })
        except DraftError as exc:
            return _err(str(exc))
        except Exception as exc:  # noqa: BLE001
            logger.exception("draft_card failed")
            return _err(f"draft_card failed: {exc}")

    def draft_card_read(args: dict, **_: Any) -> str:
        try:
            record = store_factory().get((args.get("draft_id") or "").strip())
            summary = _summary(record)
            summary["text"] = plain_text(record)
            summary["history"] = [
                {"v": v["v"], "author": v["author"], "note": v["note"]} for v in record["versions"]
            ]
            return _ok(summary)
        except DraftError as exc:
            return _err(str(exc))
        except Exception as exc:  # noqa: BLE001
            logger.exception("draft_card_read failed")
            return _err(f"draft_card_read failed: {exc}")

    def draft_card_status(args: dict, **_: Any) -> str:
        try:
            draft_id = (args.get("draft_id") or "").strip()
            record = store_factory().set_status(draft_id, (args.get("status") or "").strip(), args.get("detail") or "")
            return _ok({"draft_id": draft_id, "status": record["status"], "detail": record["status_detail"]})
        except DraftError as exc:
            return _err(str(exc))
        except Exception as exc:  # noqa: BLE001
            logger.exception("draft_card_status failed")
            return _err(f"draft_card_status failed: {exc}")

    return draft_card, draft_card_read, draft_card_status
