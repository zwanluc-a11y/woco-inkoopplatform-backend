from __future__ import annotations

import logging
import re
import time
from typing import Annotated, Optional

import jwt
from jwt import PyJWKClient
from fastapi import Depends, HTTPException, Query, Request, status
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.models.user import User
from app.models.user_organization import UserOrganization

_CLERK_ID_PATTERN = re.compile(r"^user_[a-zA-Z0-9]{20,}$")
logger = logging.getLogger(__name__)

_jwks_client: Optional[PyJWKClient] = None

# In-memory cache for Clerk user info (clerk_id -> {info, timestamp})
_clerk_info_cache: dict[str, dict] = {}
_CACHE_TTL = 600  # 10 minutes


def _get_jwks_client() -> PyJWKClient:
    global _jwks_client
    if _jwks_client is None:
        if not settings.CLERK_JWKS_URL:
            raise HTTPException(status_code=500, detail="CLERK_JWKS_URL is niet geconfigureerd")
        _jwks_client = PyJWKClient(settings.CLERK_JWKS_URL, cache_keys=True)
    return _jwks_client


def _decode_clerk_token(token: str) -> dict:
    try:
        signing_key = _get_jwks_client().get_signing_key_from_jwt(token)
        decode_options: dict = {"verify_aud": False}
        kwargs: dict = {"algorithms": ["RS256"], "options": decode_options}
        if settings.CLERK_ISSUER:
            kwargs["issuer"] = settings.CLERK_ISSUER
        else:
            decode_options["verify_iss"] = False
        payload = jwt.decode(token, signing_key.key, **kwargs)
        return payload
    except jwt.exceptions.PyJWTError as e:
        logger.debug("Clerk JWT verification failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Ongeldige authenticatie-gegevens",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _extract_user_info_from_jwt(payload: dict) -> dict:
    """Extract email and name from JWT claims only (no external calls)."""
    info: dict = {"email": "", "first_name": "", "last_name": ""}
    info["email"] = payload.get("email", "") or payload.get("primary_email_address", "")
    info["first_name"] = payload.get("first_name", "")
    info["last_name"] = payload.get("last_name", "")

    if not info["email"]:
        email_addresses = payload.get("email_addresses", [])
        if email_addresses and isinstance(email_addresses, list):
            first_email = email_addresses[0] if email_addresses else {}
            if isinstance(first_email, dict):
                info["email"] = first_email.get("email_address", "")
            elif isinstance(first_email, str):
                info["email"] = first_email
    return info


def _fetch_clerk_user_info(clerk_id: str) -> dict:
    """Fetch user info from Clerk API with caching. Only called for new users."""
    now = time.time()
    cached = _clerk_info_cache.get(clerk_id)
    if cached and (now - cached["ts"]) < _CACHE_TTL:
        return cached["info"]

    info: dict = {"email": "", "first_name": "", "last_name": ""}

    if not _CLERK_ID_PATTERN.match(clerk_id):
        return info

    clerk_secret = settings.CLERK_SECRET_KEY
    if not clerk_secret:
        try:
            from app.models.app_setting import AppSetting
            tmp_db = SessionLocal()
            row = tmp_db.query(AppSetting).filter(AppSetting.key == "CLERK_SECRET_KEY").first()
            if row:
                clerk_secret = row.value
            tmp_db.close()
        except Exception:
            pass

    if clerk_secret:
        try:
            import json as _json
            import urllib.request
            req = urllib.request.Request(
                f"https://api.clerk.com/v1/users/{clerk_id}",
                headers={"Authorization": f"Bearer {clerk_secret}"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    data = _json.loads(resp.read())
                    addrs = data.get("email_addresses", [])
                    if addrs:
                        info["email"] = addrs[0].get("email_address", "")
                    info["first_name"] = data.get("first_name", "") or ""
                    info["last_name"] = data.get("last_name", "") or ""
        except Exception as e:
            logger.warning("Failed to fetch Clerk user info: %s", e)

    _clerk_info_cache[clerk_id] = {"info": info, "ts": now}
    return info


def _resolve_user(payload: dict, db: Session) -> User:
    clerk_id = payload.get("sub")
    if not clerk_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Ongeldige token: geen gebruiker")

    # Fast path: user already exists in DB — no Clerk API call needed
    user = db.query(User).filter(User.clerk_id == clerk_id).first()
    if user:
        # Only update if name is still "Gebruiker" or email is missing
        needs_update = False
        if (not user.name or user.name == "Gebruiker") or not user.email:
            jwt_info = _extract_user_info_from_jwt(payload)
            if not user.email and jwt_info["email"]:
                user.email = jwt_info["email"]
                needs_update = True
            if (not user.name or user.name == "Gebruiker"):
                display = f"{jwt_info['first_name']} {jwt_info['last_name']}".strip()
                if not display and jwt_info["email"]:
                    display = jwt_info["email"].split("@")[0]
                if display:
                    user.name = display
                    needs_update = True

        if needs_update:
            db.commit()
            db.refresh(user)
        return user

    # Slow path: new user — extract info (may call Clerk API once)
    jwt_info = _extract_user_info_from_jwt(payload)
    email = jwt_info["email"]
    display_name = f"{jwt_info['first_name']} {jwt_info['last_name']}".strip()

    # If JWT didn't have email/name, try Clerk API (cached)
    if not email or not display_name:
        clerk_info = _fetch_clerk_user_info(clerk_id)
        if not email:
            email = clerk_info["email"]
        if not display_name:
            display_name = f"{clerk_info['first_name']} {clerk_info['last_name']}".strip()

    if not display_name and email:
        display_name = email.split("@")[0]

    # Check by email (only if we actually have one)
    if email:
        user = db.query(User).filter(User.email == email).first()
        if user:
            user.clerk_id = clerk_id
            db.commit()
            db.refresh(user)
            return user

    # Create new user — use clerk_id as fallback email to satisfy UNIQUE constraint
    if not email:
        email = f"{clerk_id}@clerk.placeholder"
    name = display_name or "Gebruiker"
    user = User(clerk_id=clerk_id, email=email, name=name, platform_role=None)
    db.add(user)
    try:
        db.commit()
    except Exception:
        db.rollback()
        # Might be a duplicate — try to find by clerk_id or email again
        user = db.query(User).filter(
            (User.clerk_id == clerk_id) | (User.email == email)
        ).first()
        if user:
            if not user.clerk_id:
                user.clerk_id = clerk_id
                db.commit()
            return user
        raise
    db.refresh(user)
    return user


def get_current_user(request: Request, db: Annotated[Session, Depends(get_db)]) -> User:
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Niet geautoriseerd",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = auth_header[7:]
    payload = _decode_clerk_token(token)
    return _resolve_user(payload, db)


def get_current_user_or_token(
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    token: Optional[str] = Query(None),
) -> User:
    auth_header = request.headers.get("Authorization")
    raw_token = None
    if auth_header and auth_header.startswith("Bearer "):
        raw_token = auth_header[7:]
    elif token:
        raw_token = token
    if not raw_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Niet geautoriseerd")
    payload = _decode_clerk_token(raw_token)
    return _resolve_user(payload, db)


def verify_org_membership(
    org_id: int,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
) -> UserOrganization:
    """Any authenticated user has access — return a dummy membership."""
    return UserOrganization(user_id=current_user.id, organization_id=org_id, role="eigenaar")


def verify_org_beheerder(org_id: int, current_user: Annotated[User, Depends(get_current_user)], db: Annotated[Session, Depends(get_db)]) -> UserOrganization:
    return verify_org_membership(org_id, current_user, db)


def verify_org_eigenaar(org_id: int, current_user: Annotated[User, Depends(get_current_user)], db: Annotated[Session, Depends(get_db)]) -> UserOrganization:
    return verify_org_membership(org_id, current_user, db)


def verify_platform_eigenaar(current_user: Annotated[User, Depends(get_current_user)]) -> User:
    return current_user


def verify_platform_user(current_user: Annotated[User, Depends(get_current_user)]) -> User:
    return current_user


verify_org_admin = verify_org_eigenaar
