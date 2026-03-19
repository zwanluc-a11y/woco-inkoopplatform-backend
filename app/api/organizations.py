from __future__ import annotations

import base64
from pathlib import Path
from typing import Annotated, Literal, Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from fastapi.responses import Response
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db, verify_org_beheerder, verify_org_eigenaar, verify_org_membership, verify_platform_user
from app.models.user_organization import UserOrganization
from app.models.category_duration_setting import CategoryDurationSetting
from app.models.contract import Contract
from app.models.import_session import ImportSession
from app.models.organization import Organization
from app.models.procurement_calendar_item import ProcurementCalendarItem
from app.models.risk_assessment import RiskAssessment
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_yearly_spend import SupplierYearlySpend
from app.models.threshold import Threshold
from app.models.transaction import Transaction
from app.models.user import User
from app.schemas.organization import (
    OrganizationCreate,
    OrganizationResponse,
    OrganizationUpdate,
    ThresholdResponse,
    ThresholdUpdate,
)

router = APIRouter(prefix="/organizations", tags=["organizations"])

# Default thresholds per organization type
DEFAULT_THRESHOLDS = {
    "rijksoverheid": {
        "2024-2025": {"diensten_leveringen": 143000, "werken": 5538000, "ict_diensten": 750000},
        "2026-2027": {"diensten_leveringen": 140000, "werken": 5404000, "ict_diensten": 750000},
    },
    "woningcorporatie_klein": {
        "2024-2025": {"diensten_leveringen": 221000, "werken": 5538000, "ict_diensten": 750000},
        "2026-2027": {"diensten_leveringen": 216000, "werken": 5404000, "ict_diensten": 750000},
    },
    "woningcorporatie_middel": {
        "2024-2025": {"diensten_leveringen": 221000, "werken": 5538000, "ict_diensten": 750000},
        "2026-2027": {"diensten_leveringen": 216000, "werken": 5404000, "ict_diensten": 750000},
    },
    "woningcorporatie_groot": {
        "2024-2025": {"diensten_leveringen": 221000, "werken": 5538000, "ict_diensten": 750000},
        "2026-2027": {"diensten_leveringen": 216000, "werken": 5404000, "ict_diensten": 750000},
    },
    "stichting": {
        "2024-2025": {"diensten_leveringen": 221000, "werken": 5538000, "ict_diensten": 750000},
        "2026-2027": {"diensten_leveringen": 216000, "werken": 5404000, "ict_diensten": 750000},
    },
    "nutssector": {
        "2024-2025": {"diensten_leveringen": 443000, "werken": 5538000, "ict_diensten": 1000000},
        "2026-2027": {"diensten_leveringen": 432000, "werken": 5404000, "ict_diensten": 1000000},
    },
    "woningcorporatie": {
        "2024-2025": {"diensten_leveringen": 221000, "werken": 5538000, "ict_diensten": 750000},
        "2026-2027": {"diensten_leveringen": 216000, "werken": 5404000, "ict_diensten": 750000},
    },
    "overig": {
        "2024-2025": {"diensten_leveringen": 221000, "werken": 5538000, "ict_diensten": 750000},
        "2026-2027": {"diensten_leveringen": 216000, "werken": 5404000, "ict_diensten": 750000},
    },
}


@router.get("/", response_model=list[OrganizationResponse])
async def list_organizations(
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    # Platform users see all organizations
    if current_user.platform_role in ("eigenaar", "beheerder"):
        return db.query(Organization).order_by(Organization.name).all()

    # Regular users see only their memberships
    return (
        db.query(Organization)
        .join(UserOrganization, UserOrganization.organization_id == Organization.id)
        .filter(UserOrganization.user_id == current_user.id)
        .all()
    )


@router.post("/", response_model=OrganizationResponse, status_code=status.HTTP_201_CREATED)
async def create_organization(
    data: OrganizationCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(verify_platform_user)],
):
    org = Organization(
        name=data.name,
        org_type=data.org_type,
        category_system="aedes",
        description=data.description,
        created_by=current_user.id,
    )
    db.add(org)
    db.flush()

    # Seed default thresholds based on org_type
    org_thresholds = DEFAULT_THRESHOLDS.get(data.org_type, DEFAULT_THRESHOLDS["overig"])
    for period, values in org_thresholds.items():
        threshold = Threshold(
            organization_id=org.id,
            threshold_period=period,
            diensten_leveringen=values["diensten_leveringen"],
            werken=values["werken"],
            ict_diensten=values["ict_diensten"],
            is_default=True,
        )
        db.add(threshold)

    # Only create UserOrganization for non-platform users
    if not current_user.platform_role:
        membership = UserOrganization(
            user_id=current_user.id,
            organization_id=org.id,
            role="eigenaar",
        )
        db.add(membership)

    db.commit()
    db.refresh(org)
    return org


@router.get("/{org_id}", response_model=OrganizationResponse, dependencies=[Depends(verify_org_membership)])
async def get_organization(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organisatie niet gevonden")
    return org


@router.put("/{org_id}", response_model=OrganizationResponse, dependencies=[Depends(verify_org_beheerder)])
async def update_organization(
    org_id: int,
    data: OrganizationUpdate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organisatie niet gevonden")
    update_data = data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(org, field, value)
    db.commit()
    db.refresh(org)
    return org


@router.get("/{org_id}/thresholds", response_model=list[ThresholdResponse], dependencies=[Depends(verify_org_membership)])
async def get_thresholds(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    return db.query(Threshold).filter(Threshold.organization_id == org_id).all()


@router.put("/{org_id}/thresholds/{threshold_id}", response_model=ThresholdResponse, dependencies=[Depends(verify_org_beheerder)])
async def update_threshold(
    org_id: int,
    threshold_id: int,
    data: ThresholdUpdate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    threshold = (
        db.query(Threshold)
        .filter(Threshold.id == threshold_id, Threshold.organization_id == org_id)
        .first()
    )
    if not threshold:
        raise HTTPException(status_code=404, detail="Drempel niet gevonden")
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(threshold, field, value)
    db.commit()
    db.refresh(threshold)
    return threshold


@router.delete("/{org_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(verify_org_eigenaar)])
async def delete_organization(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Delete an organization and ALL related data (cascade)."""
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organisatie niet gevonden")

    # Delete related data in correct order (respecting foreign keys)
    from app.models.invitation import Invitation
    db.query(Invitation).filter(Invitation.organization_id == org_id).delete()
    db.query(UserOrganization).filter(UserOrganization.organization_id == org_id).delete()
    db.query(ProcurementCalendarItem).filter(ProcurementCalendarItem.organization_id == org_id).delete()
    db.query(RiskAssessment).filter(RiskAssessment.organization_id == org_id).delete()
    db.query(Contract).filter(Contract.organization_id == org_id).delete()
    db.query(CategoryDurationSetting).filter(CategoryDurationSetting.organization_id == org_id).delete()
    db.query(SupplierCategorization).filter(SupplierCategorization.organization_id == org_id).delete()
    db.query(SupplierYearlySpend).filter(SupplierYearlySpend.organization_id == org_id).delete()
    db.query(Transaction).filter(Transaction.organization_id == org_id).delete()
    db.query(Supplier).filter(Supplier.organization_id == org_id).delete()
    db.query(Threshold).filter(Threshold.organization_id == org_id).delete()
    db.query(ImportSession).filter(ImportSession.organization_id == org_id).delete()

    db.delete(org)
    db.commit()


class BrandColorsUpdate(BaseModel):
    brand_primary_color: Optional[str] = None
    brand_secondary_color: Optional[str] = None
    brand_accent_color: Optional[str] = None


def _save_brand_upload(file_content: bytes, org_id: int, filename: str, prefix: str) -> str:
    """Save an uploaded brand image and return the file path."""
    logos_dir = Path(__file__).parent.parent.parent / "data" / "logos"
    logos_dir.mkdir(parents=True, exist_ok=True)
    # Sanitize filename: strip path components to prevent path traversal
    safe_name = Path(filename).name.replace(" ", "_") if filename else f"{prefix}.png"
    # Extra safety: remove any remaining path separators
    safe_name = safe_name.replace("/", "_").replace("\\", "_").replace("..", "_")
    file_path = logos_dir / f"{org_id}_{prefix}_{safe_name}"
    file_path.write_bytes(file_content)
    return str(file_path)


MAX_IMAGE_SIZE = 5 * 1024 * 1024  # 5 MB


def _validate_image(file: UploadFile):
    """Validate that file is an allowed image type. SVG is blocked (XSS risk)."""
    allowed_types = {"image/png", "image/jpeg", "image/jpg", "image/webp"}
    if file.content_type not in allowed_types:
        raise HTTPException(status_code=400, detail="Ongeldig bestandstype. Upload een PNG, JPEG of WebP afbeelding.")


def _brand_response(org):
    """Return standard brand response dict."""
    return {
        "logo_path": org.brand_logo_path,
        "screenshot_path": org.brand_screenshot_path,
        "primary_color": org.brand_primary_color,
        "secondary_color": org.brand_secondary_color,
        "accent_color": org.brand_accent_color,
    }


@router.post("/{org_id}/brand/logo", dependencies=[Depends(verify_org_beheerder)])
async def upload_brand_logo(
    org_id: int,
    file: Annotated[UploadFile, File(...)],
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Upload an organization logo (shown in PDF report). No color extraction."""
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organisatie niet gevonden")

    _validate_image(file)
    content = await file.read()
    if len(content) > MAX_IMAGE_SIZE:
        raise HTTPException(status_code=400, detail="Afbeelding is te groot. Maximum is 5 MB.")
    org.brand_logo_path = _save_brand_upload(content, org_id, file.filename or "logo.png", "logo")

    # Store in DB for persistence across deploys
    org.brand_logo_data = base64.b64encode(content).decode("ascii")

    db.commit()
    db.refresh(org)
    return _brand_response(org)


@router.post("/{org_id}/brand/screenshot", dependencies=[Depends(verify_org_beheerder)])
async def upload_brand_screenshot(
    org_id: int,
    file: Annotated[UploadFile, File(...)],
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Upload a website screenshot to extract brand colors."""
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organisatie niet gevonden")

    _validate_image(file)
    content = await file.read()
    if len(content) > MAX_IMAGE_SIZE:
        raise HTTPException(status_code=400, detail="Afbeelding is te groot. Maximum is 5 MB.")
    file_path = _save_brand_upload(content, org_id, file.filename or "screenshot.png", "screenshot")
    org.brand_screenshot_path = file_path

    # Store in DB for persistence across deploys
    org.brand_screenshot_data = base64.b64encode(content).decode("ascii")

    # Extract colors from screenshot
    from app.services.color_extraction_service import extract_dominant_colors

    colors = extract_dominant_colors(file_path, n=3)
    if len(colors) >= 1:
        org.brand_primary_color = colors[0]
    if len(colors) >= 2:
        org.brand_secondary_color = colors[1]
    if len(colors) >= 3:
        org.brand_accent_color = colors[2]

    db.commit()
    db.refresh(org)
    return _brand_response(org)


# Keep legacy endpoint for backwards compatibility
@router.post("/{org_id}/brand", dependencies=[Depends(verify_org_beheerder)])
async def upload_brand_legacy(
    org_id: int,
    file: Annotated[UploadFile, File(...)],
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Legacy: upload image as logo + extract colors. Use /brand/logo or /brand/screenshot instead."""
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organisatie niet gevonden")

    _validate_image(file)
    content = await file.read()
    if len(content) > MAX_IMAGE_SIZE:
        raise HTTPException(status_code=400, detail="Afbeelding is te groot. Maximum is 5 MB.")
    file_path = _save_brand_upload(content, org_id, file.filename or "logo.png", "logo")
    org.brand_logo_path = file_path

    # Store in DB for persistence across deploys
    org.brand_logo_data = base64.b64encode(content).decode("ascii")

    from app.services.color_extraction_service import extract_dominant_colors

    colors = extract_dominant_colors(file_path, n=3)
    if len(colors) >= 1:
        org.brand_primary_color = colors[0]
    if len(colors) >= 2:
        org.brand_secondary_color = colors[1]
    if len(colors) >= 3:
        org.brand_accent_color = colors[2]

    db.commit()
    db.refresh(org)
    return _brand_response(org)


@router.put("/{org_id}/brand-colors", dependencies=[Depends(verify_org_beheerder)])
async def update_brand_colors(
    org_id: int,
    data: BrandColorsUpdate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Manually update brand colors."""
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organisatie niet gevonden")
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(org, field, value)
    db.commit()
    db.refresh(org)
    return _brand_response(org)


@router.get("/{org_id}/brand", dependencies=[Depends(verify_org_membership)])
async def get_brand_info(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Get brand logo, screenshot and colors for an organization."""
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organisatie niet gevonden")

    # Restore files from DB if missing on disk (after container restart)
    _restore_from_db(org, db)

    return _brand_response(org)


def _restore_from_db(org, db):
    """Restore brand images from DB base64 data if files are missing on disk."""
    changed = False
    logos_dir = Path(__file__).parent.parent.parent / "data" / "logos"
    logos_dir.mkdir(parents=True, exist_ok=True)

    if org.brand_logo_data and (not org.brand_logo_path or not Path(org.brand_logo_path).exists()):
        try:
            restored = logos_dir / f"{org.id}_logo_restored.png"
            restored.write_bytes(base64.b64decode(org.brand_logo_data))
            org.brand_logo_path = str(restored)
            changed = True
        except Exception:
            pass

    if org.brand_screenshot_data and (not org.brand_screenshot_path or not Path(org.brand_screenshot_path).exists()):
        try:
            restored = logos_dir / f"{org.id}_screenshot_restored.png"
            restored.write_bytes(base64.b64decode(org.brand_screenshot_data))
            org.brand_screenshot_path = str(restored)
            changed = True
        except Exception:
            pass

    if changed:
        db.commit()


@router.get("/{org_id}/brand/image/{image_type}", dependencies=[Depends(verify_org_membership)])
async def get_brand_image(
    org_id: int,
    image_type: Literal["logo", "screenshot"],
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Serve a brand image (logo or screenshot) directly from DB or disk."""

    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organisatie niet gevonden")

    # Try DB data first (always available)
    data_field = org.brand_logo_data if image_type == "logo" else org.brand_screenshot_data
    if data_field:
        image_bytes = base64.b64decode(data_field)
        return Response(content=image_bytes, media_type="image/png")

    # Fallback to disk file
    path_field = org.brand_logo_path if image_type == "logo" else org.brand_screenshot_path
    if path_field and Path(path_field).exists():
        image_bytes = Path(path_field).read_bytes()
        return Response(content=image_bytes, media_type="image/png")

    raise HTTPException(status_code=404, detail=f"Geen {image_type} gevonden")
