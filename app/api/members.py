"""Organization member management API endpoints."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.models.user_organization import UserOrganization

VALID_ROLES = ("eigenaar", "beheerder", "kijker")

router = APIRouter(
    prefix="/organizations/{org_id}/members",
    tags=["members"],
)


@router.get("")
async def list_members(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """List all members of the organization.

    Returns platform staff (is_platform=True) and org-level kijkers.
    """
    result = []

    # 1. Platform staff (implicit access to all orgs)
    platform_users = (
        db.query(User)
        .filter(User.platform_role.isnot(None))
        .order_by(User.name)
        .all()
    )
    for u in platform_users:
        result.append({
            "id": u.id,
            "email": u.email,
            "name": u.name,
            "role": u.platform_role,
            "is_platform": True,
            "joined_at": u.created_at.isoformat() if u.created_at else None,
        })

    # 2. Org-level members (kijkers invited to this specific org)
    memberships = (
        db.query(UserOrganization, User)
        .join(User, User.id == UserOrganization.user_id)
        .filter(
            UserOrganization.organization_id == org_id,
            User.platform_role.is_(None),
        )
        .order_by(User.name)
        .all()
    )
    for membership, user in memberships:
        result.append({
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "role": membership.role,
            "is_platform": False,
            "joined_at": membership.joined_at.isoformat() if membership.joined_at else None,
        })

    return result


class UpdateRoleRequest(BaseModel):
    role: str


@router.put("/{user_id}/role")
async def update_member_role(
    org_id: int,
    user_id: int,
    data: UpdateRoleRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Change a member's role (eigenaar-only)."""
    # Block changes to platform users
    target_user = db.query(User).filter(User.id == user_id).first()
    if target_user and target_user.platform_role:
        raise HTTPException(
            status_code=400,
            detail="Platform teamleden worden beheerd via Team instellingen",
        )

    if data.role not in VALID_ROLES:
        raise HTTPException(
            status_code=400,
            detail=f"Rol moet een van {VALID_ROLES} zijn",
        )

    membership = (
        db.query(UserOrganization)
        .filter(
            UserOrganization.user_id == user_id,
            UserOrganization.organization_id == org_id,
        )
        .first()
    )
    if not membership:
        raise HTTPException(status_code=404, detail="Lid niet gevonden")

    # Prevent demoting last eigenaar
    if user_id == current_user.id and data.role != "eigenaar":
        eigenaar_count = (
            db.query(UserOrganization)
            .filter(
                UserOrganization.organization_id == org_id,
                UserOrganization.role == "eigenaar",
            )
            .count()
        )
        if eigenaar_count <= 1:
            raise HTTPException(
                status_code=400,
                detail="Kan de laatste eigenaar niet degraderen",
            )

    membership.role = data.role
    db.commit()
    return {"detail": "Rol bijgewerkt"}


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_member(
    org_id: int,
    user_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Remove a member from the organization (eigenaar-only)."""
    # Block removal of platform users
    target_user = db.query(User).filter(User.id == user_id).first()
    if target_user and target_user.platform_role:
        raise HTTPException(
            status_code=400,
            detail="Platform teamleden worden beheerd via Team instellingen",
        )

    membership = (
        db.query(UserOrganization)
        .filter(
            UserOrganization.user_id == user_id,
            UserOrganization.organization_id == org_id,
        )
        .first()
    )
    if not membership:
        raise HTTPException(status_code=404, detail="Lid niet gevonden")

    if membership.role == "eigenaar":
        eigenaar_count = (
            db.query(UserOrganization)
            .filter(
                UserOrganization.organization_id == org_id,
                UserOrganization.role == "eigenaar",
            )
            .count()
        )
        if eigenaar_count <= 1:
            raise HTTPException(
                status_code=400,
                detail="Kan de laatste eigenaar niet verwijderen",
            )

    db.delete(membership)
    db.commit()
