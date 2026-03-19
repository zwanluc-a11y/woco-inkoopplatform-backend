"""Settings API endpoints - stores settings in database for persistence."""
from __future__ import annotations
from typing import Annotated, Optional
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.api.deps import get_current_user, get_db, verify_platform_eigenaar
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
def get_all_settings_status(user: User = Depends(verify_platform_eigenaar), db: Session = Depends(get_db)):
    anthropic_key = get_anthropic_api_key(db)
    clerk_key = get_clerk_secret_key(db)
    return AllSettingsStatus(
        anthropic_key=ApiKeyStatus(configured=bool(anthropic_key), masked_key=_mask_key(anthropic_key) if anthropic_key else None),
        clerk_secret_key=ApiKeyStatus(configured=bool(clerk_key), masked_key=_mask_key(clerk_key) if clerk_key else None),
    )


@router.get("/api-key", response_model=ApiKeyStatus)
def get_api_key_status(user: User = Depends(verify_platform_eigenaar), db: Session = Depends(get_db)):
    key = get_anthropic_api_key(db)
    return ApiKeyStatus(configured=bool(key), masked_key=_mask_key(key) if key else None)


@router.put("/api-key")
def update_api_key(data: ApiKeyUpdate, user: User = Depends(verify_platform_eigenaar), db: Session = Depends(get_db)):
    new_key = data.api_key.strip()
    if not new_key.startswith("sk-ant-"):
        return {"success": False, "error": "Ongeldige API key. Moet beginnen met 'sk-ant-'."}
    set_setting(db, "ANTHROPIC_API_KEY", new_key)
    settings.ANTHROPIC_API_KEY = new_key
    return {"success": True, "masked_key": _mask_key(new_key), "message": "API key opgeslagen."}


@router.get("/clerk-key", response_model=ApiKeyStatus)
def get_clerk_key_status(user: User = Depends(verify_platform_eigenaar), db: Session = Depends(get_db)):
    key = get_clerk_secret_key(db)
    return ApiKeyStatus(configured=bool(key), masked_key=_mask_key(key) if key else None)


@router.put("/clerk-key")
def update_clerk_key(data: ApiKeyUpdate, user: User = Depends(verify_platform_eigenaar), db: Session = Depends(get_db)):
    new_key = data.api_key.strip()
    if not new_key.startswith("sk_"):
        return {"success": False, "error": "Ongeldige Clerk key. Moet beginnen met 'sk_'."}
    set_setting(db, "CLERK_SECRET_KEY", new_key)
    settings.CLERK_SECRET_KEY = new_key
    return {"success": True, "masked_key": _mask_key(new_key), "message": "Clerk key opgeslagen."}
