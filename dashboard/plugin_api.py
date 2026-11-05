"""Routes des brouillons pour l'app, montées sous ``/api/plugins/hermes-mobile-notify/``.

Reprises de hermes-draft-cards (voir ``drafts.py``). Rien n'est envoyé ici.
"""

# No ``from __future__ import annotations`` here: the pydantic request models must
# resolve their field types however this file is imported.
import importlib.util
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field


def _load_store_module():
    """Load the sibling ``drafts.py`` by path: this file is imported standalone, not as a package."""
    path = Path(__file__).resolve().parent.parent / "drafts.py"
    spec = importlib.util.spec_from_file_location("hermes_mobile_drafts_api", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"hermes-mobile-notify: cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_store = _load_store_module()
router = APIRouter()


class EditBody(BaseModel):
    base_version: int
    card: int
    fields: dict[str, Any] = Field(default_factory=dict)


class ApproveBody(BaseModel):
    version: int


class StateBody(BaseModel):
    status: str
    detail: Optional[str] = ""


def _call(fn, *args, **kwargs) -> dict:
    try:
        return fn(*args, **kwargs)
    except _store.DraftError as exc:
        raise HTTPException(status_code=exc.code, detail=str(exc)) from exc


_EDITABLE = ("from", "to", "cc", "bcc", "subject", "body")


@router.get("/drafts/{draft_id}")
def get_draft(draft_id: str) -> dict:
    return _call(_store.DraftStore().get, draft_id)


@router.post("/drafts/{draft_id}/edit")
def edit_draft(draft_id: str, body: EditBody) -> dict:
    fields = {k: v for k, v in body.fields.items() if k in _EDITABLE}
    return _call(
        _store.DraftStore().revise,
        draft_id,
        fields=fields,
        author="user",
        card=body.card,
        base_version=body.base_version,
        note="Edited by you",
    )


@router.post("/drafts/{draft_id}/approve")
def approve_draft(draft_id: str, body: ApproveBody) -> dict:
    return _call(_store.DraftStore().approve, draft_id, body.version)


@router.post("/drafts/{draft_id}/state")
def set_state(draft_id: str, body: StateBody) -> dict:
    return _call(_store.DraftStore().set_status, draft_id, body.status, body.detail or "", by="user")
