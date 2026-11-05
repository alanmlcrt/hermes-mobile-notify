"""Plugin Hermes Mobile : notification FCM avec aperçu à chaque fin de tour.

- ``/hermes-mobile-token <jeton>`` mémorise le jeton FCM du téléphone dans
  ``HERMES_HOME/hermes-mobile/fcm-token`` (l'app l'envoie par ``command.dispatch``
  avant chaque ``prompt.submit``).
- ``on_session_end`` journalise le tour (métadonnées seulement) et lance l'envoi
  FCM dans un fil démon : le hook rend la main sans attendre le réseau.

Le compte de service vient de la variable ``HERMES_MOBILE_SERVICE_ACCOUNT_JSON``
(réglage « secret » du plugin, JSON brut ou base64) si elle est définie, sinon de
``HERMES_HOME/hermes-mobile/service-account.json`` (hors dossier du plugin).
Notification avec sujet et extrait de réponse ; ``data`` ne porte que ``outcome`` et
``sessionId``. Le contenu est lu en lecture seule dans state.db, jamais journalisé.
- ``pre_tool_call`` fait valider chaque envoi de mail par une carte dans l'app :
  la règle vit ici, donc réinstaller le plugin suffit sur un nouveau Hermes.
- ``/hermes-mobile-mail-ok`` retient l'empreinte d'un brouillon validé dans l'app
  (plugin ``draft-cards``) : ce mail-là part sans seconde carte.
Le journal ``session-end.jsonl`` (dossier du plugin) et ``agent.log``
ne gardent du jeton que ses 8 derniers caractères. Aucune erreur ne remonte au tour.

Erreurs FCM : un seul nouvel essai (après ``_RETRY_DELAY``) sur erreur de
transport ou HTTP 5xx ; ``UNREGISTERED`` supprime ``fcm-token`` s'il porte
encore ce jeton (l'app le renverra au prochain message). Pas de file durable.
"""

import base64
import hashlib
import hashlib
import json
import logging
import os
import re
import shlex
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("hermes_mobile_notify")

_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session-end.jsonl")
_STATE_DIR = "hermes-mobile"
_TOKEN_FILE = "fcm-token"
_KEY_FILE = "service-account.json"
_KEY_ENV = "HERMES_MOBILE_SERVICE_ACCOUNT_JSON"
_TIMEOUT = 8.0
_RETRY_DELAY = 2.0  # secondes avant l'unique nouvel essai (patché par l'auto-vérification)
_MAX_ATTEMPTS = 2
_SCOPE = "https://www.googleapis.com/auth/firebase.messaging"
_TOKEN_URI = "https://oauth2.googleapis.com/token"

# Repris de contracts/notification-tracking.json (payload.notification / android).
_TITLE = "Hermes"
_BODIES = {
    "succeeded": "Hermes a terminé",
    "failed": "Hermes a échoué",
    "stopped": "Hermes s'est arrêté",
}
# Fins de routine (platform=cron) : libellé distinct, même payload data.
_ROUTINE_BODIES = {
    "succeeded": "Routine terminée",
    "failed": "Routine en échec",
    "stopped": "Routine arrêtée",
}
_CHANNEL = "hermes_turn_complete"

# Carte de mail : `description` de l'autorisation = préfixe + JSON, lu par l'app
# (lib/ui/components/approval_card.dart).
_MAIL_CARD_PREFIX = "hermes-mobile-mail:"
_MAIL_COMMAND = re.compile(r"\bgmail\s+(?:send|reply)\b")
# Autres chemins vers Gmail : l'agent est renvoyé vers la commande vérifiable.
# ponytail: filet par motifs, pas une barrière ; un script maison qui appelle
# l'API autrement passe. Barrière réelle = retirer l'accès direct au jeton.
_MAIL_BYPASS = re.compile(
    r"gws\s+gmail\b.*\bsend\b|gmail\.googleapis\.com\S*messages/send|\.messages\(\)\s*\.send\(",
    re.DOTALL,
)
_MAIL_FLAGS = {
    "--to": "to",
    "--cc": "cc",
    "--from": "from",
    "--subject": "subject",
    "--body": "body",
    "--thread-id": "thread_id",
}
_SHELL_OPERATORS = set(";&|()<>\n")
_MAIL_RETRY = (
    "Email not sent. Run the Google Workspace script as a single `gmail send --to ... "
    "--subject ... --body ...` (or `gmail reply MESSAGE_ID --body ...`) command, with "
    "literal values: no $variables, $(...), backticks or files. The user must see the "
    "exact message on a confirmation card."
)
_MAIL_PROMPT = (
    "Sending email: run `gmail send` or `gmail reply` directly, with literal argument "
    "values. Hermes shows the user a confirmation card with the exact message before "
    "it leaves, so do not ask for confirmation in chat first."
)
_DRAFT_PROMPT = (
    "Drafts: when the user asks you to write or draft an email or message without "
    "sending it yet, call `draft_card` if that tool exists. This app renders cards like "
    "the desktop app: write one short line, then the returned `card` line alone on its "
    "own line, and never `text_fallback` or the draft text. The user edits and approves "
    "on the card; the app then posts a message naming the draft and its version, and "
    "you send it as the draft-cards skill describes."
)
# Empreintes des brouillons validés dans l'app : usage unique, 15 min.
_MAIL_OK_FILE = "mail-approved.json"
_MAIL_OK_TTL = 900
_MAIL_OK_USAGE = "Usage : /hermes-mobile-mail-ok <empreinte> [empreinte]"
_MAIL_ADDRESS = re.compile(r"[^\s<>,;\"]+@[^\s<>,;\"]+")
_mail_ok_lock = threading.Lock()
_TTL = "86400s"

_ALLOWED = (
    "session_id",
    "task_id",
    "turn_id",
    "completed",
    "failed",
    "interrupted",
    "turn_exit_reason",
    "model",
    "platform",
    "telemetry_schema_version",
)
_SCALARS = (str, int, float, bool, type(None))
_USAGE = "usage : /hermes-mobile-token <jeton FCM>"

# Clés de la réponse (celles que l'app sait lire) -> provider Hermes.
_QUOTA_PROVIDERS = (("claude", "anthropic"), ("codex", "openai-codex"))
_QUOTA_TTL = 60.0  # secondes : l'app relit à chaque retour sur l'accueil

_journal_lock = threading.Lock()
_quota_lock = threading.Lock()
_quota_cache = (0.0, None)
_token_lock = threading.Lock()  # écriture du jeton par la commande / oubli par le fil d'envoi
_credentials_lock = threading.Lock()
_credentials_cache = {}  # chemin -> (mtime, credentials, project_id)


def register(ctx):
    """Enregistre le hook de fin de tour et la commande du jeton."""
    ctx.register_hook("on_session_end", _on_session_end)
    ctx.register_command(
        "hermes-mobile-token",
        _cmd_token,
        description="Hermes Mobile : mémorise le jeton FCM du téléphone.",
        args_hint="<jeton>",
    )
    ctx.register_command(
        "hermes-mobile-quotas",
        _cmd_quotas,
        description="Hermes Mobile : quotas 5h et 7 jours des comptes IA, en JSON.",
    )
    ctx.register_command(
        "hermes-mobile-mail-ok",
        _cmd_mail_ok,
        description="Hermes Mobile : brouillon de mail validé dans l'app.",
        args_hint="<empreinte> [empreinte]",
    )
    ctx.register_hook("pre_tool_call", _on_pre_tool_call)
    # Absente des builds anciens : la carte fonctionne sans, l'agent confirme alors aussi dans le chat.
    if hasattr(ctx, "register_system_prompt_section"):
        ctx.register_system_prompt_section("hermes-mobile-mail", _mail_prompt)


# --- chemins ---------------------------------------------------------------


def _hermes_home():
    try:
        from hermes_constants import get_hermes_home

        return Path(get_hermes_home())
    except Exception:
        return Path(os.environ.get("HERMES_HOME") or Path.home() / ".hermes")


def _state_dir():
    return _hermes_home() / _STATE_DIR


def _read_secret():
    """Valeur du réglage secret, ou ``""``.

    ``get_secret`` est résolu dans le fil du hook : un fil d'envoi neuf n'hérite
    pas du contexte de secrets du tour. Si l'API existe mais refuse faute de
    scope (multiplex), on ne lit jamais l'environnement global : le fichier du
    profil reste la seule autre source. L'environnement n'est lu qu'avec un
    Hermes qui ne fournit pas ``agent.secret_scope`` (tests, versions antérieures).
    """
    try:
        from agent.secret_scope import get_secret
    except Exception:
        return (os.environ.get(_KEY_ENV) or "").strip()
    try:
        return (get_secret(_KEY_ENV) or "").strip()
    except Exception:
        return ""


def _service_account(state):
    """Source du compte de service : réglage secret, sinon fichier du profil."""
    raw = _read_secret()
    if raw:
        return ("env", raw)
    path = state / _KEY_FILE
    if path.is_file():
        return ("file", str(path))
    return None


def _tail(token):
    return token[-8:] if isinstance(token, str) else None


def _journal(record):
    line = json.dumps(record, ensure_ascii=False)
    with _journal_lock:
        with open(_LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


# --- commande /hermes-mobile-token ------------------------------------------


def _cmd_token(raw_args=""):
    token = (raw_args or "").strip() if isinstance(raw_args, str) else ""
    if not token or len(token) > 4096 or any(c.isspace() for c in token):
        return _USAGE
    try:
        state = _state_dir()
        state.mkdir(parents=True, exist_ok=True)
        path = state / _TOKEN_FILE
        with _token_lock:
            try:
                previous = path.read_text(encoding="utf-8").strip()
            except OSError:
                previous = None
            changed = previous != token
            if changed:
                tmp = path.with_name(_TOKEN_FILE + ".tmp")
                tmp.write_text(token, encoding="utf-8")
                try:
                    os.chmod(tmp, 0o600)
                except OSError:
                    pass
                os.replace(tmp, path)
        try:
            _journal({"ts": _now(), "event": "token", "token_tail": _tail(token), "changed": changed})
        except Exception:
            pass
        if changed:
            logger.info("hermes-mobile-notify jeton enregistré (…%s)", _tail(token))
        return "ok"
    except Exception as exc:
        try:
            logger.warning("hermes-mobile-notify: jeton non enregistré: %s", type(exc).__name__)
        except Exception:
            pass
        return "erreur : jeton non enregistré"


# --- quotas de compte ---------------------------------------------------------


def _account_usage(provider):
    # Le jeton reste dans Hermes : seul le pourcentage sort de ce fichier.
    from agent.account_usage import fetch_account_usage

    return fetch_account_usage(provider, read_only=True)


def _quota_windows(snapshot):
    quota = {}
    for window in getattr(snapshot, "windows", None) or ():
        label = str(window.label).lower()
        key = "session" if "session" in label else "weekly" if "week" in label else None
        # Les fenêtres `model` (Opus, Sonnet) ne disent rien du compte entier.
        if key and window.scope == "account" and window.used_percent is not None:
            quota[key] = {"usedPercent": min(100.0, max(0.0, float(window.used_percent)))}
    return quota


def _cmd_quotas(raw_args=""):
    global _quota_cache
    with _quota_lock:
        read_at, cached = _quota_cache
        if cached is not None and time.monotonic() - read_at < _QUOTA_TTL:
            return cached
        quotas = {}
        for key, provider in _QUOTA_PROVIDERS:
            try:
                quotas[key] = _quota_windows(_account_usage(provider))
            except Exception as exc:
                logger.warning("hermes-mobile-notify: quota %s illisible: %s", provider, type(exc).__name__)
                quotas[key] = {}
        result = json.dumps(quotas)
        _quota_cache = (time.monotonic(), result)
        return result


# --- hook on_session_end ----------------------------------------------------


def _shell_tokens(command):
    """Mots d'une commande shell, opérateurs à part ; ``ValueError`` si les guillemets sont ouverts."""
    lexer = shlex.shlex(
        command.replace("\\\n", " "),
        posix=True,
        punctuation_chars="".join(sorted(_SHELL_OPERATORS)),
    )
    lexer.whitespace = " \t\r"
    lexer.whitespace_split = True
    lexer.commenters = ""
    return list(lexer)


def _literal(value):
    # ponytail: les guillemets sont perdus ici, donc un « $5 » littéral est refusé
    # comme une expansion ; l'agent reformule, le mail affiché reste le mail envoyé.
    return value is not None and "`" not in value and not re.search(r"\$[\w({]", value)


def _parse_mail(command):
    """Mail porté par ``command``, ou ``None`` s'il ne peut pas être montré tel qu'il partira."""
    try:
        tokens = _shell_tokens(command)
    except ValueError:
        return None
    starts = [
        i for i in range(len(tokens) - 1)
        if tokens[i] == "gmail" and tokens[i + 1] in ("send", "reply")
    ]
    if len(starts) != 1:
        return None
    kind = tokens[starts[0] + 1]
    mail = {"kind": kind}
    rest = iter(tokens[starts[0] + 2:])
    for token in rest:
        if token and set(token) <= _SHELL_OPERATORS:
            break
        flag, equals, inline = token.partition("=")
        if token == "--html":
            mail["html"] = True
        elif flag in _MAIL_FLAGS:
            value = inline if equals else next(rest, None)
            if not _literal(value):
                return None
            mail[_MAIL_FLAGS[flag]] = value
        elif kind == "reply" and "message_id" not in mail and _literal(token):
            mail["message_id"] = token
        else:
            return None
    required = ("to", "subject", "body") if kind == "send" else ("message_id", "body")
    return mail if all(key in mail for key in required) else None


def _mail_prompt(session_info):
    # Seule l'app (`tui`) sait afficher `::draft-card` ; ailleurs le skill garde `text_fallback`.
    return f"{_MAIL_PROMPT}\n{_DRAFT_PROMPT}" if session_info.get("platform") == "tui" else _MAIL_PROMPT


def _digest(*parts):
    return hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()


def _addresses(value):
    return ",".join(sorted(a.lower() for a in _MAIL_ADDRESS.findall(value or "")))


def _mail_fingerprint(mail):
    """Même calcul que ``mailFingerprints`` côté app ; une réponse n'a que son corps à comparer."""
    body = mail["body"].replace("\r\n", "\n").strip()
    if mail["kind"] == "reply":
        return _digest(body)
    return _digest(_addresses(mail["to"]), _addresses(mail.get("cc")), mail["subject"].strip(), body)


def _approved_mails():
    now = time.time()
    try:
        saved = json.loads((_state_dir() / _MAIL_OK_FILE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return {k: v for k, v in saved.items() if v > now} if isinstance(saved, dict) else {}


def _save_approved_mails(approved):
    state = _state_dir()
    state.mkdir(parents=True, exist_ok=True)
    (state / _MAIL_OK_FILE).write_text(json.dumps(approved), encoding="utf-8")


def _cmd_mail_ok(raw_args=""):
    digests = raw_args.split() if isinstance(raw_args, str) else []
    if not digests or len(digests) > 2 or not all(re.fullmatch(r"[0-9a-f]{64}", d) for d in digests):
        return _MAIL_OK_USAGE
    with _mail_ok_lock:
        approved = _approved_mails()
        approved.update(dict.fromkeys(digests, time.time() + _MAIL_OK_TTL))
        _save_approved_mails(approved)
    return "ok"


def _take_approved_mail(mail):
    # ponytail: même limite que les motifs ci-dessus, un agent qui écrit ce fichier
    # passerait ; il ne peut pas appeler la commande, et le statut `approved` de
    # draft-cards ne suffit pas car l'agent peut le poser lui-même.
    digest = _mail_fingerprint(mail)
    with _mail_ok_lock:
        approved = _approved_mails()
        if digest not in approved:
            return False
        del approved[digest]
        _save_approved_mails(approved)
    return True


def _on_pre_tool_call(tool_name="", args=None, **_):
    """Carte pour un envoi de mail ; refus s'il ne peut pas être montré (Hermes ignore une erreur de hook)."""
    try:
        key = {"terminal": "command", "execute_code": "code"}.get(tool_name)
        text = (args or {}).get(key) if key else None
        if not isinstance(text, str):
            return None
        if tool_name == "terminal" and _MAIL_COMMAND.search(text):
            mail = _parse_mail(text)
            if mail is not None:
                if _take_approved_mail(mail):
                    return None
                return {
                    "action": "approve",
                    "message": _MAIL_CARD_PREFIX + json.dumps(mail, ensure_ascii=False),
                }
        elif not (_MAIL_COMMAND.search(text) or _MAIL_BYPASS.search(text)):
            return None
    except Exception:
        logger.exception("hermes-mobile-notify: lecture de l'envoi de mail impossible")
    return {"action": "block", "message": _MAIL_RETRY}


def _outcome(kwargs):
    if kwargs.get("interrupted") is True:
        return "stopped"
    if kwargs.get("failed") is True or kwargs.get("completed") is not True:
        return "failed"
    return "succeeded"


def _spawn(target, args):
    thread = threading.Thread(target=target, args=args, name="hermes-mobile-notify", daemon=True)
    thread.start()
    return thread


def _on_session_end(**kwargs):
    started = time.monotonic()
    try:
        record = {"ts": _now(), "event": "session_end"}
        for key in _ALLOWED:
            if key in kwargs:
                value = kwargs[key]
                record[key] = value if isinstance(value, _SCALARS) else type(value).__name__
        extra = sorted(k for k in kwargs if k not in _ALLOWED)
        if extra:
            record["extra_keys"] = extra
        record["outcome"] = _outcome(kwargs)
        if record.get("platform") not in ("tui", "cron"):
            # Tours hors app (ex. sous-agents « subagent ») : pas de notification.
            _finish(record, "skipped", None, "platform", started)
            return
        if not kwargs.get("turn_id") and record["outcome"] == "stopped":
            # Fermeture de session (session détachée récoltée après la fin du
            # tour, app quittée) : pas une fin de tour, ne pas notifier « arrêté ».
            # ponytail: seule forme observée (interrupted, sans turn_id) ; une fin
            # réussie ou échouée sans turn_id reste notifiée.
            del record["outcome"]
            _finish(record, "skipped", None, "session_close", started)
            return
        # Résolu ici : un fil neuf ne voit pas la surcharge de HERMES_HOME du tour.
        state = _state_dir()
        key = _service_account(state)
        _spawn(_deliver, (record, state, key, started, time.time()))
    except Exception as exc:  # jamais fatal pour le tour
        try:
            logger.warning("hermes-mobile-notify: hook en échec: %s", type(exc).__name__)
        except Exception:
            pass


def _deliver(record, state, key, started, ended_at):
    try:
        try:
            action, status, reason = _send(record, state, key, ended_at)
        except Exception as exc:
            action, status, reason = "failed", None, type(exc).__name__
        _finish(record, action, status, reason, started)
    except Exception:
        pass


def _finish(record, action, status, reason, started):
    try:
        record["action"] = action
        record["status"] = status
        if reason:
            record["reason"] = reason
        record["duration_ms"] = round((time.monotonic() - started) * 1000)
        try:
            _journal(record)
        except Exception as exc:
            logger.warning("hermes-mobile-notify: journal impossible: %s", type(exc).__name__)
        logger.info(
            "hermes-mobile-notify session_id=%s platform=%s outcome=%s action=%s status=%s reason=%s duration_ms=%s",
            record.get("session_id"),
            record.get("platform"),
            record.get("outcome"),
            action,
            status,
            reason,
            record["duration_ms"],
        )
    except Exception:
        pass


def _send(record, state, key, ended_at):
    session_id = record.get("session_id")
    if not isinstance(session_id, str) or not session_id or len(session_id) > 256:
        return "skipped", None, "no_session_id"
    try:
        token = (state / _TOKEN_FILE).read_text(encoding="utf-8").strip()
    except OSError:
        token = ""
    if not token:
        return "skipped", None, "no_token"
    record["token_tail"] = _tail(token)
    if key is None:
        return "skipped", None, "no_key"
    try:
        credentials, project_id = _load_credentials(key)
    except Exception:
        return "failed", None, "key_invalid"
    preview = _session_preview(state.parent, session_id, ended_at, record["outcome"])
    body = _payload(token, session_id, record["outcome"], record.get("platform") == "cron", preview)
    url = f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send"
    attempt = 0
    while True:
        attempt += 1
        record["attempts"] = attempt
        last = attempt >= _MAX_ATTEMPTS
        try:
            status, fcm_error = _post(url, body, credentials)
        except Exception as exc:
            if last:
                return "failed", None, type(exc).__name__
            time.sleep(_RETRY_DELAY)
            continue
        if 200 <= status < 300:
            return "sent", status, None
        if fcm_error == "UNREGISTERED":
            _forget_token(state, token)
            return "failed", status, fcm_error
        if status >= 500 and not last:
            time.sleep(_RETRY_DELAY)
            continue
        return "failed", status, fcm_error or "fcm_http"


def _forget_token(state, token):
    """Supprime ``fcm-token`` s'il contient encore ``token`` (jamais un jeton renvoyé depuis)."""
    path = state / _TOKEN_FILE
    try:
        with _token_lock:
            if path.read_text(encoding="utf-8").strip() == token:
                path.unlink()
                logger.info("hermes-mobile-notify jeton refusé par FCM oublié (…%s)", _tail(token))
    except OSError:
        pass


def _excerpt(value, max_bytes):
    """Texte visible seulement, Markdown allégé et taille bornée en UTF-8."""
    if isinstance(value, list):
        value = " ".join(part.get("text", "") for part in value
                         if isinstance(part, dict) and part.get("type") in ("text", "output_text")
                         and isinstance(part.get("text"), str))
    if not isinstance(value, str):
        return ""
    value = re.sub(r"<think>.*?(?:</think>|$)", "", value, flags=re.S | re.I)
    value = re.sub(r"```.*?(?:```|$)", "", value, flags=re.S)
    value = re.sub(r"::draft-card\{[^}]*\}", "", value)
    value = re.sub(r"!?\[([^\]]+)\]\([^)]*\)", r"\1", value)
    value = re.sub(r"(?m)^\s*(?:#{1,6}\s+|[-*>]\s+|\d+[.)]\s+)", "", value)
    value = re.sub(r"[*_`]+", "", value)
    value = " ".join(value.split())
    value = "".join(c for c in value if c.isprintable())
    encoded = value.encode("utf-8", errors="replace")
    if len(encoded) <= max_bytes:
        return value
    return encoded[:max_bytes - 3].decode("utf-8", errors="ignore").rstrip() + "…"


def _session_preview(home, session_id, ended_at, outcome):
    """Lit uniquement le dernier tour du profil ; tout échec garde le libellé fixe."""
    db = None
    try:
        db = sqlite3.connect((home / "state.db").resolve().as_uri() + "?mode=ro", uri=True, timeout=0.2)
        deadline = time.monotonic() + 0.2
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        db.execute("BEGIN")
        user = db.execute(
            "SELECT id, substr(content, 1, 16000) FROM messages WHERE session_id = ? AND role = 'user' "
            "AND timestamp <= ? ORDER BY id DESC LIMIT 1", (session_id, ended_at),
        ).fetchone()
        if user is None:
            return None
        # Ne jamais reprendre la réponse d'un tour précédent ou d'un appel d'outil.
        answer = db.execute(
            "SELECT substr(content, 1, 16000) FROM messages WHERE session_id = ? AND role = 'assistant' "
            "AND id > ? AND timestamp <= ? AND (tool_calls IS NULL OR tool_calls IN ('', '[]', 'null')) "
            "ORDER BY id DESC LIMIT 1", (session_id, user[0], ended_at),
        ).fetchone() if outcome == "succeeded" else None

        def content(raw, limit):
            if isinstance(raw, str) and raw.lstrip().startswith('['):
                try:
                    raw = json.loads(raw)
                except ValueError:
                    return ""
            return _excerpt(raw, limit)

        return content(user[1], 100), content(answer[0], 600) if answer else ""
    except Exception:
        return None
    finally:
        if db is not None:
            db.close()


def _payload(token, session_id, outcome, routine=False, preview=None):
    title = _TITLE
    body = (_ROUTINE_BODIES if routine else _BODIES)[outcome]
    if preview:
        task, answer = preview
        if task:
            title = f"{'Routine' if routine else _TITLE} · {task}"
        if answer and outcome == "succeeded":
            body = answer
    return {
        "message": {
            "token": token,
            "notification": {"title": title, "body": body},
            "data": {"outcome": outcome, "sessionId": session_id},
            "android": {
                "priority": "high",
                "ttl": _TTL,
                "collapse_key": f"hermes:{session_id}",
                "notification": {
                    "channel_id": _CHANNEL,
                    "notification_priority": "PRIORITY_HIGH",
                    "sound": "default",
                    "tag": f"hermes:{session_id}:{outcome}",
                },
            },
        }
    }


# --- transport FCM HTTP v1 (remplacé par l'auto-vérification) ---------------


def _service_account_info(source):
    """JSON du compte de service, depuis le fichier ou la variable (brut/base64)."""
    kind, value = source
    if kind == "file":
        with open(value, encoding="utf-8") as fh:
            raw = json.load(fh)
    else:
        text = value.strip()
        if not text.startswith("{"):
            # Base64 collé depuis un outil qui coupe les lignes : ignorer les blancs.
            text = base64.b64decode(re.sub(r"\s+", "", text), validate=True).decode("utf-8")
        raw = json.loads(text)
    if not isinstance(raw, dict):
        raise ValueError("service_account")
    return raw


def _google_credentials(source):
    kind, value = source
    if kind == "file":
        stamp = os.stat(value).st_mtime
        cache_key = value
    else:
        stamp = None
        cache_key = "env:" + hashlib.sha256(value.encode("utf-8")).hexdigest()
    with _credentials_lock:
        cached = _credentials_cache.get(cache_key)
        if cached and cached[0] == stamp:
            return cached[1], cached[2]
        from google.oauth2 import service_account

        raw = _service_account_info(source)
        # Refuser un endpoint OAuth arbitraire dans la source fournie.
        if raw.get("token_uri") != _TOKEN_URI:
            raise ValueError("token_uri")
        credentials = service_account.Credentials.from_service_account_info(raw, scopes=[_SCOPE])
        project_id = credentials.project_id
        if not isinstance(project_id, str) or not re.fullmatch(r"[a-z][a-z0-9-]{4,61}[a-z0-9]", project_id):
            raise ValueError("project_id")
        _credentials_cache[cache_key] = (stamp, credentials, project_id)
        return credentials, project_id


def _http_post(url, body, credentials):
    import requests
    from google.auth.transport.requests import Request

    with _credentials_lock:
        if not credentials.valid:
            request = Request()
            credentials.refresh(lambda **kw: request(**{**kw, "timeout": _TIMEOUT}))
        bearer = credentials.token
    response = requests.post(
        url,
        data=json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json; charset=UTF-8"},
        timeout=_TIMEOUT,
        allow_redirects=False,
    )
    return response.status_code, _fcm_error(response)


def _fcm_error(response):
    """Code d'erreur FCM (énuméré, ex. ``UNREGISTERED``) ; jamais le texte libre."""
    if 200 <= response.status_code < 300:
        return None
    try:
        error = response.json().get("error") or {}
        codes = [d.get("errorCode") for d in error.get("details") or () if isinstance(d, dict)]
        for code in codes + [error.get("status")]:
            if isinstance(code, str) and re.fullmatch(r"[A-Z_]{1,40}", code):
                return code
    except Exception:
        pass
    return None


_load_credentials = _google_credentials
_post = _http_post


# --- auto-vérification --------------------------------------------------------


def _self_check_mail():
    def decide(command, tool="terminal", key="command"):
        return _on_pre_tool_call(tool_name=tool, args={key: command})

    def card(command):
        decision = decide(command)
        assert decision and decision["action"] == "approve", decision
        assert decision["message"].startswith(_MAIL_CARD_PREFIX)
        return json.loads(decision["message"][len(_MAIL_CARD_PREFIX):])

    gapi = 'GAPI="python /h/skills/google_api.py"\n$GAPI '
    assert card(gapi + """gmail send --to a@b.fr --subject 'Été; ok' --body "L1
L2 && pas une commande" --cc c@d.fr --html""") == {
        "kind": "send", "to": "a@b.fr", "subject": "Été; ok",
        "body": "L1\nL2 && pas une commande", "cc": "c@d.fr", "html": True,
    }
    assert card("cd /tmp && python g.py gmail send \\\n --to=a@b.fr --subject s --body b && echo fini") == {
        "kind": "send", "to": "a@b.fr", "subject": "s", "body": "b",
    }
    assert card("$GAPI gmail reply 18f --body Merci") == {
        "kind": "reply", "message_id": "18f", "body": "Merci",
    }
    # Ce qui ne peut pas être montré tel quel ne part pas.
    for command in (
        '$GAPI gmail send --to a@b.fr --subject s --body "$(cat f.txt)"',
        '$GAPI gmail send --to "$DEST" --subject s --body b',
        "$GAPI gmail send --to a@b.fr --subject s --body `cat f`",
        "$GAPI gmail send --to a@b.fr --subject s",
        "$GAPI gmail send --to a@b.fr --subject s --body b --attach x",
        "$GAPI gmail send --to a@b.fr --subject 'ouvert --body b",
        "$GAPI gmail send --to a --subject s --body b; $GAPI gmail send --to c --subject s --body b",
        "bash -c 'python g.py gmail send --to a --subject s --body b'",
        "gws gmail users messages send --params x",
        "curl https://gmail.googleapis.com/gmail/v1/users/me/messages/send -d x",
    ):
        assert decide(command)["action"] == "block", command
    assert decide("service.users().messages().send(userId='me')", "execute_code", "code")["action"] == "block"
    assert decide("subprocess.run('g.py gmail send --to a', shell=True)", "execute_code", "code")["action"] == "block"
    # Le reste passe sans carte.
    for command in ("ls -la", "$GAPI gmail search 'is:unread'", "$GAPI gmail get 18f", "gws gmail users messages list"):
        assert decide(command) is None, command
    assert _on_pre_tool_call(tool_name="read_file", args={"path": "gmail send"}) is None
    assert _on_pre_tool_call(tool_name="terminal", args=None) is None

    # Brouillon validé dans l'app : ce mail exact part une fois, sans seconde carte.
    send = "$GAPI gmail send --to 'Ana <A@b.fr>' --subject Devis --body 'Bonjour'"
    fingerprint = _mail_fingerprint({"kind": "send", "to": "a@b.fr", "subject": "Devis", "body": "Bonjour\n"})
    # Même valeur attendue dans test/draft_card_test.dart.
    assert fingerprint == "f142c04b27383d7e5d092f96a4c6459f39b9bc99b05f3907f7195ebb766546f3"
    assert _cmd_mail_ok("pas-une-empreinte") == _MAIL_OK_USAGE
    assert _cmd_mail_ok(fingerprint + " " + _digest("Merci")) == "ok"
    assert decide(send.replace("Bonjour", "Autre"))["action"] == "approve"
    assert decide(send) is None
    assert decide(send)["action"] == "approve"
    assert decide("$GAPI gmail reply 18f --body Merci") is None
    (_state_dir() / _MAIL_OK_FILE).write_text(json.dumps({fingerprint: time.time() - 1}), encoding="utf-8")
    assert decide(send)["action"] == "approve"
    assert _DRAFT_PROMPT in _mail_prompt({"platform": "tui"})
    assert _DRAFT_PROMPT not in _mail_prompt({"platform": "telegram"})
    assert _excerpt('Voici.\n::draft-card{id="d_1a2b3c4d" v="1"}', 100) == "Voici."


def _self_check_quotas(cmd):
    global _account_usage, _quota_cache
    from types import SimpleNamespace as Row

    saved = _account_usage
    reads = []

    def usage(provider):
        reads.append(provider)
        if provider == "anthropic":
            raise RuntimeError("no account")
        return Row(windows=(
            Row(label="Session", used_percent=12.5, scope="account"),
            Row(label="Weekly", used_percent=140, scope="account"),
            Row(label="Opus week", used_percent=90, scope="model"),
            Row(label="Monthly", used_percent=5, scope="account"),
        ))

    _account_usage, _quota_cache = usage, (0.0, None)
    try:
        expected = {
            "claude": {},
            "codex": {"session": {"usedPercent": 12.5}, "weekly": {"usedPercent": 100.0}},
        }
        assert json.loads(cmd("")) == expected
        assert json.loads(cmd("")) == expected and len(reads) == 2, "cache de 60 s"
        assert _quota_windows(None) == {}
    finally:
        _account_usage, _quota_cache = saved, (0.0, None)


def _self_check():
    import tempfile
    from contextlib import closing

    global _LOG_PATH, _hermes_home, _load_credentials, _post, _spawn, _RETRY_DELAY, _read_secret
    saved = (_LOG_PATH, _hermes_home, _load_credentials, _post, _spawn, _RETRY_DELAY, _read_secret)
    saved_key_env = os.environ.pop(_KEY_ENV, None)
    token = "synthetic-ABCDEFGH"
    hooks, commands, sent, threads, records, spawned = {}, {}, [], [], [], []

    class _Ctx:
        def register_hook(self, name, cb):
            hooks[name] = cb

        def register_command(self, name, handler, description="", args_hint=""):
            commands[name] = handler

    class _Capture(logging.Handler):
        def emit(self, rec):
            records.append(rec)

    def spawn(target, args):
        thread = saved[4](target, args)
        threads.append(thread)
        spawned.append(thread)
        return thread

    def end_turn(**kwargs):
        kwargs.setdefault("platform", "tui")
        kwargs.setdefault("turn_id", "u")
        started = time.monotonic()
        hooks["on_session_end"](**kwargs)
        elapsed = time.monotonic() - started
        for thread in threads:
            thread.join(5)
        threads.clear()
        with open(_LOG_PATH, encoding="utf-8") as fh:
            return json.loads(fh.readlines()[-1]), elapsed

    register(_Ctx())
    assert set(hooks) == {"on_session_end", "pre_tool_call"}
    assert set(commands) == {"hermes-mobile-token", "hermes-mobile-quotas", "hermes-mobile-mail-ok"}
    _self_check_quotas(commands["hermes-mobile-quotas"])
    cmd, capture = commands["hermes-mobile-token"], _Capture()
    logger.addHandler(capture)
    logger.setLevel(logging.INFO)
    with tempfile.TemporaryDirectory() as tmp:
        try:
            home = Path(tmp) / "home"
            state = home / _STATE_DIR
            _LOG_PATH = os.path.join(tmp, "session-end.jsonl")
            _hermes_home = lambda: home
            _spawn = spawn
            _RETRY_DELAY = 0.05
            # Le test ne dépend pas d'un Hermes installé (agent.secret_scope).
            _read_secret = lambda: (os.environ.get(_KEY_ENV) or "").strip()

            def post(url, body, credentials):
                sent.append((url, body))
                return 200, None

            _load_credentials = lambda path: ("creds", "hermes-mobile-test")
            _post = post

            # Pas de jeton : rien n'est envoyé.
            line, _ = end_turn(session_id="s1", completed=True, failed=False, interrupted=False, platform="tui")
            assert (line["action"], line["reason"], line["status"]) == ("skipped", "no_token", None), line
            assert not sent

            # Commande : argument vide → usage ; jeton → « ok », stocké.
            assert cmd("") == _USAGE and cmd("   ") == _USAGE and cmd("a b") == _USAGE
            assert cmd(f"  {token}\n") == "ok"
            assert (state / _TOKEN_FILE).read_text(encoding="utf-8") == token
            assert cmd(token) == "ok"

            # Pas de clé : rien n'est envoyé.
            line, _ = end_turn(session_id="s1", completed=True)
            assert (line["action"], line["reason"]) == ("skipped", "no_key"), line
            assert line["token_tail"] == token[-8:]
            assert not sent

            # Réglage secret : JSON brut ou base64, prioritaire sur le fichier.
            info = {"type": "service_account", "project_id": "hermes-mobile-test", "token_uri": _TOKEN_URI}
            raw_info = json.dumps(info, separators=(",", ":"))
            assert _service_account_info(("env", raw_info)) == info
            encoded = base64.b64encode(raw_info.encode("utf-8")).decode("ascii")
            folded = f"  {encoded[:16]}\n{encoded[16:]}  \n"
            assert _service_account_info(("env", folded)) == info
            os.environ[_KEY_ENV] = raw_info
            assert _service_account(state) == ("env", raw_info)
            del os.environ[_KEY_ENV]
            assert _service_account(state) is None

            # Jeton + clé : envoi, sans attendre le réseau.
            (state / _KEY_FILE).write_text("{}", encoding="utf-8")
            assert _service_account(state) == ("file", str(state / _KEY_FILE))

            def slow_post(url, body, credentials):
                time.sleep(0.5)
                return post(url, body, credentials)

            _post = slow_post
            line, elapsed = end_turn(
                session_id="20261029_212234_ce7c72", task_id="t", turn_id="u", completed=True,
                failed=False, interrupted=False, turn_exit_reason="text_response(finish_reason=stop)",
                model="m", platform="tui", messages=["secret"],
            )
            assert elapsed < 0.2, elapsed
            assert (line["action"], line["status"], line["outcome"]) == ("sent", 200, "succeeded"), line
            assert line["attempts"] == 1, line
            assert line["duration_ms"] >= 500 and line["extra_keys"] == ["messages"], line
            url, body = sent[-1]
            assert url == "https://fcm.googleapis.com/v1/projects/hermes-mobile-test/messages:send"
            message = body["message"]
            assert message["token"] == token
            assert message["notification"] == {"title": "Hermes", "body": "Hermes a terminé"}
            assert message["data"] == {"outcome": "succeeded", "sessionId": "20261029_212234_ce7c72"}
            assert message["android"]["notification"]["channel_id"] == "hermes_turn_complete"
            assert "secret" not in json.dumps(body)
            _post = post

            # Arrêt et échec.
            line, _ = end_turn(session_id="s2", completed=False, interrupted=True)
            assert (line["action"], line["outcome"]) == ("sent", "stopped"), line
            assert sent[-1][1]["message"]["notification"]["body"] == "Hermes s'est arrêté"
            line, _ = end_turn(session_id="s2", completed=False, failed=True, interrupted=False)
            assert line["outcome"] == "failed" and sent[-1][1]["message"]["data"]["outcome"] == "failed"

            # Fermeture de session (sans turn_id, observée en réel 8 s après la fin) : ni fil ni envoi.
            before, forks = len(sent), len(spawned)
            line, _ = end_turn(session_id="s2", completed=False, interrupted=True, turn_id=None)
            assert (line["action"], line["reason"]) == ("skipped", "session_close") and "outcome" not in line, line
            assert len(sent) == before and len(spawned) == forks
            # Fin réussie sans turn_id : notifiée quand même.
            line, _ = end_turn(session_id="s2", completed=True, turn_id=None)
            assert (line["action"], line["outcome"]) == ("sent", "succeeded"), line

            # Sous-agent (ou toute plateforme hors app) : journalisé, ni fil ni envoi.
            before, forks = len(sent), len(spawned)
            for platform in ("subagent", "telegram"):
                line, _ = end_turn(session_id="s2-child", completed=True, platform=platform)
                assert (line["action"], line["reason"], line["platform"]) == ("skipped", "platform", platform), line
            assert len(sent) == before and len(spawned) == forks

            # Fin de routine : notifiée, libellé de routine.
            line, _ = end_turn(session_id="s2-cron", completed=True, platform="cron")
            assert line["action"] == "sent", line
            assert sent[-1][1]["message"]["notification"]["body"] == "Routine terminée"

            # Aperçu réel depuis SQLite : bon profil, dernier tour, texte visible seulement.
            db_path = home / "state.db"
            with closing(sqlite3.connect(db_path)) as db, db:
                db.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, "
                           "role TEXT, content TEXT, tool_calls TEXT, timestamp REAL)")
                db.executemany("INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?)", [
                    (1, "preview", "user", "Ancienne tâche", None, 1),
                    (2, "preview", "assistant", "Ancienne réponse", None, 2),
                    (3, "other", "assistant", "Autre conversation", None, 3),
                    (4, "preview", "user", "**Prépare mon voyage**", None, 4),
                    (5, "preview", "assistant", "Je cherche", '[{"name":"search"}]', 5),
                    (6, "preview", "tool", "Résultat brut", None, 6),
                    (7, "preview", "assistant", json.dumps([
                        {"type": "thinking", "text": "Raisonnement privé"},
                        {"type": "text", "text": "## Voyage prêt\n\nVoici [le programme](https://example.com)."},
                    ]), None, 7),
                    (8, "preview", "user", "Prochain tour", None, time.time() + 3600),
                ])
            line, _ = end_turn(session_id="preview", completed=True)
            notification = sent[-1][1]["message"]["notification"]
            assert notification == {"title": "Hermes · Prépare mon voyage", "body": "Voyage prêt Voici le programme."}, notification
            assert "Voyage prêt" not in json.dumps(line)
            assert _session_preview(home, "preview", 6, "succeeded") == ("Prépare mon voyage", "")
            assert _session_preview(home, "preview", 9, "failed") == ("Prépare mon voyage", "")
            assert _session_preview(home, "absent", 9, "succeeded") is None
            assert _session_preview(home / "absent", "preview", 9, "succeeded") is None
            assert _excerpt('<think>privé</think>**Fait**\n```py\nsecret\n```', 600) == "Fait"
            assert len(_excerpt("😀é" * 1000, 600).encode("utf-8")) <= 600
            # Pas de contenu ancien si le dernier tour n'a aucune réponse.
            with closing(sqlite3.connect(db_path)) as db, db:
                db.execute("INSERT INTO messages VALUES (9, 'preview', 'user', 'Nouvelle tâche', NULL, 9)")
            assert _session_preview(home, "preview", 10, "succeeded") == ("Nouvelle tâche", "")
            worst = _payload("t" * 200, "s" * 256, "succeeded", preview=("x" * 100, "x" * 600))
            assert len(json.dumps(worst, ensure_ascii=False).encode("utf-8")) < 4096
            db_path.unlink()

            token_path = state / _TOKEN_FILE

            def scripted(*answers):
                """Transport rejouant ``answers`` : (statut, code) ou exception."""
                calls = []

                def fake(url, body, credentials):
                    calls.append(body)
                    answer = answers[min(len(calls), len(answers)) - 1]
                    if isinstance(answer, Exception):
                        raise answer
                    if callable(answer):
                        return answer()
                    return answer

                return fake, calls

            # Refus non transitoire (4xx autre qu'UNREGISTERED) : aucun nouvel essai, jeton gardé.
            _post, calls = scripted((400, "INVALID_ARGUMENT"))
            line, _ = end_turn(session_id="s3", completed=True)
            assert (line["action"], line["status"], line["reason"], line["attempts"]) == (
                "failed", 400, "INVALID_ARGUMENT", 1), line
            assert len(calls) == 1 and token_path.read_text(encoding="utf-8") == token

            # Erreur réseau puis succès : un seul nouvel essai, après le délai.
            _post, calls = scripted(ConnectionError("synthetic"), (200, None))
            line, _ = end_turn(session_id="s3", completed=True)
            assert (line["action"], line["status"], line["attempts"]) == ("sent", 200, 2), line
            assert len(calls) == 2 and line["duration_ms"] >= 50, line

            # HTTP 503 puis succès : nouvel essai aussi.
            _post, calls = scripted((503, "UNAVAILABLE"), (200, None))
            line, _ = end_turn(session_id="s3", completed=True)
            assert (line["action"], line["attempts"]) == ("sent", 2) and len(calls) == 2, line

            # Deux erreurs réseau : échec, pas de troisième essai, aucune exception.
            _post, calls = scripted(ConnectionError("synthetic"))
            line, _ = end_turn(session_id="s3", completed=True)
            assert (line["action"], line["status"], line["reason"], line["attempts"]) == (
                "failed", None, "ConnectionError", 2), line
            assert len(calls) == 2

            # 429 : refus 4xx, échec sans nouvel essai, jeton gardé.
            _post, calls = scripted((429, "QUOTA_EXCEEDED"))
            line, _ = end_turn(session_id="s3", completed=True)
            assert (line["action"], line["status"], line["attempts"]) == ("failed", 429, 1), line
            assert len(calls) == 1 and token_path.read_text(encoding="utf-8") == token

            # UNREGISTERED : jeton oublié, sans nouvel essai ; le tour suivant n'envoie rien.
            _post, calls = scripted((404, "UNREGISTERED"))
            line, _ = end_turn(session_id="s3", completed=True)
            assert (line["action"], line["status"], line["reason"], line["attempts"]) == (
                "failed", 404, "UNREGISTERED", 1), line
            assert len(calls) == 1 and not token_path.exists()
            line, _ = end_turn(session_id="s3", completed=True)
            assert (line["action"], line["reason"]) == ("skipped", "no_token"), line
            assert len(calls) == 1

            # UNREGISTERED alors que l'app a renvoyé un autre jeton entre-temps : le nouveau est gardé.
            assert cmd(token) == "ok"
            fresh = "synthetic-fresh-token-9876543210-ZYXWVUTS"

            def resent():
                assert cmd(fresh) == "ok"
                return 404, "UNREGISTERED"

            _post, calls = scripted(resent)
            line, _ = end_turn(session_id="s3", completed=True)
            assert (line["action"], line["reason"]) == ("failed", "UNREGISTERED"), line
            assert token_path.read_text(encoding="utf-8") == fresh
            assert cmd(token) == "ok"
            _post = post

            def bad_key(path):
                raise ValueError("synthetic")

            _load_credentials = bad_key
            line, _ = end_turn(session_id="s3", completed=True)
            assert (line["action"], line["reason"]) == ("failed", "key_invalid"), line

            # Sans session_id : ignoré.
            line, _ = end_turn(completed=True)
            assert (line["action"], line["reason"]) == ("skipped", "no_session_id"), line

            # Journal illisible : le hook et le fil ne lèvent rien.
            _LOG_PATH = tmp
            hooks["on_session_end"](session_id="s4", completed=True, platform="tui")
            for thread in threads:
                thread.join(5)
            threads.clear()
            _LOG_PATH = os.path.join(tmp, "session-end.jsonl")

            # Le jeton complet n'apparaît ni dans le journal ni dans agent.log.
            with open(_LOG_PATH, encoding="utf-8") as fh:
                assert token not in fh.read()
            logged = "\n".join(rec.getMessage() for rec in records)
            assert token not in logged and token[-8:] in logged
            assert "Voyage prêt" not in logged and "Prépare mon voyage" not in logged

            # Code d'erreur FCM : seulement une valeur énumérée.
            class _Resp:
                status_code = 404

                def json(self):
                    return {"error": {"status": "NOT_FOUND", "message": "texte libre",
                                      "details": [{"errorCode": "UNREGISTERED"}]}}

            assert _fcm_error(_Resp()) == "UNREGISTERED"
            _Resp.json = lambda self: {"error": {"status": "free text"}}
            assert _fcm_error(_Resp()) is None
        finally:
            _LOG_PATH, _hermes_home, _load_credentials, _post, _spawn, _RETRY_DELAY, _read_secret = saved
            os.environ.pop(_KEY_ENV, None)
            if saved_key_env is not None:
                os.environ[_KEY_ENV] = saved_key_env
            logger.removeHandler(capture)
    with tempfile.TemporaryDirectory() as tmp:
        saved_home, _hermes_home = _hermes_home, lambda: Path(tmp)
        try:
            _self_check_mail()
        finally:
            _hermes_home = saved_home
    print("hermes-mobile-notify: auto-vérification réussie")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _self_check()
