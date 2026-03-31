"""Settings API endpoints - stores settings in database for persistence."""
from __future__ import annotations
from typing import Annotated, Optional
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.api.deps import get_current_user, get_db
from app.config import settings
from app.models.app_setting import AppSetting
from app.models.user import User

router = APIRouter(prefix="/settings", tags=["settings"])


class ApiKeyStatus(BaseModel):
    configured: bool
    masked_key: Optional[str] = None


class ApiKeyUpdate(BaseModel):
    api_key: str


class AllSettingsStatus(BaseModel):
    anthropic_key: ApiKeyStatus
    clerk_secret_key: ApiKeyStatus


def _mask_key(key: str) -> str:
    if not key or len(key) < 8:
        return "***"
    return "..." + key[-4:]


def get_setting(db: Session, key: str) -> Optional[str]:
    row = db.query(AppSetting).filter(AppSetting.key == key).first()
    return row.value if row else None


def set_setting(db: Session, key: str, value: str) -> None:
    row = db.query(AppSetting).filter(AppSetting.key == key).first()
    if row:
        row.value = value
    else:
        db.add(AppSetting(key=key, value=value))
    db.commit()


def get_anthropic_api_key(db: Session) -> Optional[str]:
    env_key = settings.ANTHROPIC_API_KEY
    if env_key and env_key != "sk-ant-VULL-HIER-JE-KEY-IN":
        return env_key
    return get_setting(db, "ANTHROPIC_API_KEY")


def get_clerk_secret_key(db: Session) -> Optional[str]:
    db_key = get_setting(db, "CLERK_SECRET_KEY")
    if db_key:
        return db_key
    return settings.CLERK_SECRET_KEY or None


@router.get("/status", response_model=AllSettingsStatus)
def get_all_settings_status(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    anthropic_key = get_anthropic_api_key(db)
    clerk_key = get_clerk_secret_key(db)
    return AllSettingsStatus(
        anthropic_key=ApiKeyStatus(configured=bool(anthropic_key), masked_key=_mask_key(anthropic_key) if anthropic_key else None),
        clerk_secret_key=ApiKeyStatus(configured=bool(clerk_key), masked_key=_mask_key(clerk_key) if clerk_key else None),
    )


@router.get("/api-key", response_model=ApiKeyStatus)
def get_api_key_status(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    key = get_anthropic_api_key(db)
    return ApiKeyStatus(configured=bool(key), masked_key=_mask_key(key) if key else None)


@router.put("/api-key")
def update_api_key(data: ApiKeyUpdate, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    new_key = data.api_key.strip()
    if not new_key.startswith("sk-ant-"):
        return {"success": False, "error": "Ongeldige API key. Moet beginnen met 'sk-ant-'."}
    set_setting(db, "ANTHROPIC_API_KEY", new_key)
    settings.ANTHROPIC_API_KEY = new_key
    return {"success": True, "masked_key": _mask_key(new_key), "message": "API key opgeslagen."}


@router.get("/clerk-key", response_model=ApiKeyStatus)
def get_clerk_key_status(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    key = get_clerk_secret_key(db)
    return ApiKeyStatus(configured=bool(key), masked_key=_mask_key(key) if key else None)


@router.put("/clerk-key")
def update_clerk_key(data: ApiKeyUpdate, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    new_key = data.api_key.strip()
    if not new_key.startswith("sk_"):
        return {"success": False, "error": "Ongeldige Clerk key. Moet beginnen met 'sk_'."}
    set_setting(db, "CLERK_SECRET_KEY", new_key)
    settings.CLERK_SECRET_KEY = new_key
    return {"success": True, "masked_key": _mask_key(new_key), "message": "Clerk key opgeslagen."}


# ---------------------------------------------------------------------------
# Domain-based auto-access whitelist
# ---------------------------------------------------------------------------
import json as _json
import re as _re

_DOMAIN_RE = _re.compile(r"^[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?(\.[a-zA-Z]{2,})+$")
VALID_AUTO_ACCESS_ROLES = ("beheerder", "eigenaar")


class AutoAccessDomainsResponse(BaseModel):
    domains: list[str]
    default_role: str
    upgraded_users: int = 0


class AutoAccessDomainsUpdate(BaseModel):
    domains: list[str]
    default_role: str = "beheerder"


def get_auto_access_domains(db: Session) -> list[str]:
    """Return the list of whitelisted email domains."""
    raw = get_setting(db, "AUTO_ACCESS_DOMAINS")
    if not raw:
        return []
    try:
        domains = _json.loads(raw)
        return [d.lower().strip() for d in domains if d.strip()]
    except (ValueError, TypeError):
        return []


def get_auto_access_role(db: Session) -> str:
    """Return the default platform role for auto-access users."""
    role = get_setting(db, "AUTO_ACCESS_ROLE")
    return role if role in VALID_AUTO_ACCESS_ROLES else "beheerder"


@router.get("/auto-access-domains", response_model=AutoAccessDomainsResponse)
def get_domains(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return AutoAccessDomainsResponse(
        domains=get_auto_access_domains(db),
        default_role=get_auto_access_role(db),
    )


@router.put("/auto-access-domains", response_model=AutoAccessDomainsResponse)
def update_domains(
    data: AutoAccessDomainsUpdate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if data.default_role not in VALID_AUTO_ACCESS_ROLES:
        return {"error": f"Rol moet een van {VALID_AUTO_ACCESS_ROLES} zijn"}

    # Validate and normalize domains
    clean: list[str] = []
    for d in data.domains:
        d = d.lower().strip().lstrip("@")
        if not d:
            continue
        if not _DOMAIN_RE.match(d):
            return {"error": f"Ongeldig domein: {d}"}
        clean.append(d)

    # Remove duplicates, keep order
    seen: set[str] = set()
    unique: list[str] = []
    for d in clean:
        if d not in seen:
            seen.add(d)
            unique.append(d)

    set_setting(db, "AUTO_ACCESS_DOMAINS", _json.dumps(unique))
    set_setting(db, "AUTO_ACCESS_ROLE", data.default_role)

    # Auto-upgrade existing users whose email domain matches a newly added domain
    upgraded = _upgrade_existing_users(db, unique, data.default_role) if unique else 0

    return AutoAccessDomainsResponse(domains=unique, default_role=data.default_role, upgraded_users=upgraded)


def _upgrade_existing_users(db: Session, domains: list[str], role: str) -> int:
    """Set platform_role for existing users without one whose email matches a whitelisted domain."""
    from app.models.user import User as _User
    users_without_role = db.query(_User).filter(
        _User.platform_role.is_(None),
        _User.email.isnot(None),
    ).all()
    count = 0
    for u in users_without_role:
        if u.email and "@" in u.email:
            domain = u.email.rsplit("@", 1)[1].lower()
            if domain in domains:
                u.platform_role = role
                count += 1
    if count:
        db.commit()
    return count
