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
def get_me(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
):
    # Re-check domain whitelist in case it was set after user creation
    if not current_user.platform_role and current_user.email:
        from app.api.deps import _check_domain_whitelist
        auto_role = _check_domain_whitelist(current_user.email, db)
        if auto_role:
            current_user.platform_role = auto_role
            db.commit()
            db.refresh(current_user)
    return current_user


@router.put("/me", response_model=UserResponse)
def update_me(
    body: dict,
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
):
    """Sync user profile from Clerk (name/email)."""
    updated = False
    if "name" in body and body["name"] and (not current_user.name or current_user.name == "Gebruiker"):
        current_user.name = body["name"]
        updated = True
    if "email" in body and body["email"] and not current_user.email:
        current_user.email = body["email"]
        updated = True
    if updated:
        db.commit()
        db.refresh(current_user)
    return current_user


@router.post("/promote-first-admin")
def promote_first_admin(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
):
    """Promote the current user to platform eigenaar (only works if no eigenaar exists yet)."""
    existing_owner = db.query(User).filter(User.platform_role == "eigenaar").first()
    if existing_owner:
        raise HTTPException(status_code=409, detail="Er is al een platform eigenaar")
    current_user.platform_role = "eigenaar"
    db.commit()
    db.refresh(current_user)
    return {"detail": f"Gebruiker {current_user.email} is nu platform eigenaar", "user": {"id": current_user.id, "email": current_user.email, "platform_role": current_user.platform_role}}


@router.get("/debug-users")
def debug_users(
    current_user: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
):
    """Debug: list all users with their platform_role (eigenaar only)."""
    if current_user.platform_role != "eigenaar":
        raise HTTPException(status_code=403, detail="Alleen platform eigenaar")
    from app.api.settings import get_auto_access_domains, get_auto_access_role
    domains = get_auto_access_domains(db)
    role = get_auto_access_role(db)
    users = db.query(User).order_by(User.id).all()
    return {
        "whitelist": {"domains": domains, "default_role": role},
        "users": [
            {
                "id": u.id,
                "email": u.email,
                "name": u.name,
                "platform_role": u.platform_role,
                "clerk_id": u.clerk_id[:10] + "..." if u.clerk_id else None,
                "domain_match": (
                    u.email.rsplit("@", 1)[1].lower() in domains
                    if u.email and "@" in u.email
                    else False
                ),
            }
            for u in users
        ],
    }


@router.post("/bootstrap")
def bootstrap_admin(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Bootstrap: promote current user to eigenaar (only works if no eigenaar exists yet)."""
    existing_owner = db.query(User).filter(User.platform_role == "eigenaar").first()
    if existing_owner and existing_owner.email and existing_owner.clerk_id:
        raise HTTPException(status_code=409, detail="Er is al een platform eigenaar")
    # Demote ghost record (no email or no clerk_id)
    if existing_owner and (not existing_owner.email or not existing_owner.clerk_id):
        existing_owner.platform_role = None
        db.flush()
    current_user.platform_role = "eigenaar"
    db.commit()
    db.refresh(current_user)
    return {"detail": "Je bent nu platform eigenaar", "platform_role": "eigenaar"}


@router.get("/invite/{token}")
def get_invitation_info(token: str, db: Annotated[Session, Depends(get_db)]):
    invitation = db.query(Invitation).filter(
        Invitation.token == token, Invitation.is_used == False, Invitation.expires_at > datetime.utcnow()
    ).first()
    if not invitation:
        raise HTTPException(status_code=404, detail="Uitnodiging niet gevonden of verlopen")
    org = db.query(Organization).filter(Organization.id == invitation.organization_id).first()
    return {"organization_name": org.name if org else "Onbekend", "role": invitation.role, "expires_at": invitation.expires_at.isoformat()}


@router.post("/invite/{token}/accept")
def accept_invitation(token: str, db: Annotated[Session, Depends(get_db)], current_user: Annotated[User, Depends(get_current_user)]):
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
