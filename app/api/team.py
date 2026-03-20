"""Platform team management API endpoints (platform eigenaar only)."""
from __future__ import annotations

from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db
from app.models.user import User

VALID_PLATFORM_ROLES = ("eigenaar", "beheerder")

router = APIRouter(prefix="/team", tags=["team"])


@router.get("", dependencies=[Depends(get_current_user)])
async def list_team_members(
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """List all Inkada platform users (eigenaar + beheerder)."""
    members = (
        db.query(User)
        .filter(User.platform_role.isnot(None))
        .order_by(User.name)
        .all()
    )
    return [
        {
            "id": u.id,
            "email": u.email,
            "name": u.name,
            "platform_role": u.platform_role,
            "created_at": u.created_at.isoformat() if u.created_at else None,
        }
        for u in members
    ]


class TeamInviteRequest(BaseModel):
    email: str
    platform_role: str = "beheerder"
    name: Optional[str] = None


@router.post(
    "/invite",
    dependencies=[Depends(get_current_user)],
    status_code=status.HTTP_201_CREATED,
)
async def invite_team_member(
    data: TeamInviteRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Invite a user to the Inkada platform team.

    If the user already exists (by email), sets their platform_role.
    If not, creates a stub user that will be linked on Clerk login.
    """
    if data.platform_role not in VALID_PLATFORM_ROLES:
        raise HTTPException(
            status_code=400,
            detail=f"platform_role moet een van {VALID_PLATFORM_ROLES} zijn",
        )

    existing = db.query(User).filter(User.email == data.email).first()
    if existing:
        if existing.platform_role:
            raise HTTPException(status_code=409, detail="Deze gebruiker is al een teamlid")
        existing.platform_role = data.platform_role
        db.commit()
        return {"detail": "Teamlid toegevoegd", "user_id": existing.id}

    new_user = User(
        email=data.email,
        name=data.name or data.email.split("@")[0],
        platform_role=data.platform_role,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return {"detail": "Teamlid uitgenodigd", "user_id": new_user.id}


class UpdatePlatformRoleRequest(BaseModel):
    platform_role: str


@router.put("/{user_id}/role", dependencies=[Depends(get_current_user)])
async def update_team_member_role(
    user_id: int,
    data: UpdatePlatformRoleRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Change a team member's platform role."""
    if data.platform_role not in VALID_PLATFORM_ROLES:
        raise HTTPException(
            status_code=400,
            detail=f"platform_role moet een van {VALID_PLATFORM_ROLES} zijn",
        )

    target = db.query(User).filter(User.id == user_id).first()
    if not target or not target.platform_role:
        raise HTTPException(status_code=404, detail="Teamlid niet gevonden")

    if user_id == current_user.id and data.platform_role != "eigenaar":
        eigenaar_count = db.query(User).filter(User.platform_role == "eigenaar").count()
        if eigenaar_count <= 1:
            raise HTTPException(
                status_code=400,
                detail="Kan de laatste platform eigenaar niet degraderen",
            )

    target.platform_role = data.platform_role
    db.commit()
    return {"detail": "Rol bijgewerkt"}


@router.delete(
    "/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(get_current_user)],
)
async def remove_team_member(
    user_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Remove a user from the platform team (sets platform_role to null)."""
    target = db.query(User).filter(User.id == user_id).first()
    if not target or not target.platform_role:
        raise HTTPException(status_code=404, detail="Teamlid niet gevonden")

    if user_id == current_user.id:
        raise HTTPException(status_code=400, detail="Kan jezelf niet uit het team verwijderen")

    if target.platform_role == "eigenaar":
        eigenaar_count = db.query(User).filter(User.platform_role == "eigenaar").count()
        if eigenaar_count <= 1:
            raise HTTPException(
                status_code=400,
                detail="Kan de laatste platform eigenaar niet verwijderen",
            )

    target.platform_role = None
    db.commit()
