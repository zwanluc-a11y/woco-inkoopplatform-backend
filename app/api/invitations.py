"""Invitation management API endpoints (eigenaar-only)."""
from __future__ import annotations

import secrets
from datetime import datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db
from app.config import settings
from app.models.invitation import Invitation
from app.models.user import User

limiter = Limiter(key_func=get_remote_address)

VALID_ROLES = ("kijker",)  # Platform staff is managed via /team
MAX_INVITATION_EXPIRY_DAYS = 30  # Maximum invitation validity

router = APIRouter(
    prefix="/organizations/{org_id}/invitations",
    tags=["invitations"],
)


class CreateInvitationRequest(BaseModel):
    role: str = "kijker"
    expires_in_days: int = Field(default=7, ge=1, le=MAX_INVITATION_EXPIRY_DAYS)


@router.post("", status_code=status.HTTP_201_CREATED)
@limiter.limit("10/hour")
async def create_invitation(
    request: Request,  # required by slowapi
    org_id: int,
    data: CreateInvitationRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Generate a new invitation link."""
    if data.role not in VALID_ROLES:
        raise HTTPException(
            status_code=400,
            detail=f"Rol moet een van {VALID_ROLES} zijn",
        )

    token = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(days=data.expires_in_days)

    invitation = Invitation(
        organization_id=org_id,
        token=token,
        role=data.role,
        created_by_id=current_user.id,
        expires_at=expires_at,
    )
    db.add(invitation)
    db.commit()
    db.refresh(invitation)

    invite_url = f"{settings.FRONTEND_URL}/invite/{token}"

    # Only show full token + URL in the create response (one-time)
    return {
        "id": invitation.id,
        "token": invitation.token,
        "role": invitation.role,
        "invite_url": invite_url,
        "expires_at": invitation.expires_at.isoformat(),
        "is_used": invitation.is_used,
        "created_at": invitation.created_at.isoformat(),
    }


@router.get("")
async def list_invitations(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """List all invitations for this organization.

    Tokens are masked in the list response for security.
    Full tokens are only shown once at creation time.
    """
    invitations = (
        db.query(Invitation)
        .filter(Invitation.organization_id == org_id)
        .order_by(Invitation.created_at.desc())
        .all()
    )
    return [
        {
            "id": inv.id,
            "token_hint": inv.token[-6:] if inv.token else "",
            "role": inv.role,
            "expires_at": inv.expires_at.isoformat(),
            "is_used": inv.is_used,
            "is_expired": inv.expires_at < datetime.utcnow(),
            "created_at": inv.created_at.isoformat(),
        }
        for inv in invitations
    ]


@router.delete("/{invitation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_invitation(
    org_id: int,
    invitation_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Delete an invitation."""
    inv = (
        db.query(Invitation)
        .filter(Invitation.id == invitation_id, Invitation.organization_id == org_id)
        .first()
    )
    if not inv:
        raise HTTPException(status_code=404, detail="Uitnodiging niet gevonden")
    db.delete(inv)
    db.commit()
