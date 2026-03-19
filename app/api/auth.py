"""Authentication endpoints - Clerk-based."""
from datetime import datetime
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.api.deps import get_current_user, get_db
from app.models.invitation import Invitation
from app.models.organization import Organization
from app.models.user import User
from app.models.user_organization import UserOrganization
from app.schemas.user import UserResponse

router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: Annotated[User, Depends(get_current_user)]):
    return current_user


@router.get("/invite/{token}")
async def get_invitation_info(token: str, db: Annotated[Session, Depends(get_db)]):
    invitation = db.query(Invitation).filter(
        Invitation.token == token, Invitation.is_used == False, Invitation.expires_at > datetime.utcnow()
    ).first()
    if not invitation:
        raise HTTPException(status_code=404, detail="Uitnodiging niet gevonden of verlopen")
    org = db.query(Organization).filter(Organization.id == invitation.organization_id).first()
    return {"organization_name": org.name if org else "Onbekend", "role": invitation.role, "expires_at": invitation.expires_at.isoformat()}


@router.post("/invite/{token}/accept")
async def accept_invitation(token: str, db: Annotated[Session, Depends(get_db)], current_user: Annotated[User, Depends(get_current_user)]):
    invitation = db.query(Invitation).filter(
        Invitation.token == token, Invitation.is_used == False, Invitation.expires_at > datetime.utcnow()
    ).first()
    if not invitation:
        raise HTTPException(status_code=404, detail="Uitnodiging niet gevonden of verlopen")
    existing = db.query(UserOrganization).filter(
        UserOrganization.user_id == current_user.id, UserOrganization.organization_id == invitation.organization_id
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="Je bent al lid van deze organisatie")
    membership = UserOrganization(user_id=current_user.id, organization_id=invitation.organization_id, role=invitation.role)
    db.add(membership)
    invitation.is_used = True
    invitation.used_by_id = current_user.id
    invitation.used_at = datetime.utcnow()
    db.commit()
    return {"detail": "Je bent nu lid van de organisatie"}
