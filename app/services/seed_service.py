"""
Seed helpers – called once at application startup.

* seed_woningcorporaties – imports 259 Dutch housing corporations
* seed_inkoop_categories – imports PIANOo + WoCo sector categories
* seed_leveranciers – imports 1179 known suppliers into master database
* seed_user_organizations – backfills UserOrganization for existing data
* seed_platform_eigenaar – ensures at least one platform admin
"""

import json
import logging
import re
from pathlib import Path

from sqlalchemy.orm import Session

from app.models.category import InkoopCategory
from app.models.supplier_master_category import SupplierMasterCategory
from app.models.user_organization import UserOrganization
from app.models.woningcorporatie import WoningCorporatie

logger = logging.getLogger(__name__)

_SEED_DIR = Path(__file__).resolve().parent.parent.parent / "data"
AEDES_JSON_PATH = _SEED_DIR / "categories" / "aedes_categories.json"
BU_WOCO_JSON_PATH = _SEED_DIR / "categories" / "bu_woco_categories.json"
WOCO_JSON_PATH = _SEED_DIR / "categories" / "woco_categories.json"
CORPORATIES_JSON_PATH = _SEED_DIR / "corporaties.json"
LEVERANCIERS_JSON_PATH = _SEED_DIR / "leveranciers.json"


def _normalize_name(name: str) -> str:
    """Normalize a supplier name for matching."""
    n = name.lower().strip()
    n = re.sub(r"\b(b\.?v\.?|n\.?v\.?|v\.?o\.?f\.?|c\.?v\.?)\b", "", n)
    n = re.sub(r"[^a-z0-9\s]", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


# ── Woningcorporaties ───────────────────────────────────────────────
def seed_woningcorporaties(db: Session) -> None:
    """Seed the 259 Dutch housing corporations from bronbestand."""
    count = db.query(WoningCorporatie).count()
    if count > 0:
        logger.info("woningcorporaties table has %d rows – skipping seed.", count)
        return

    if not CORPORATIES_JSON_PATH.exists():
        logger.warning("No corporaties seed data at '%s'", CORPORATIES_JSON_PATH)
        return

    with open(CORPORATIES_JSON_PATH, "r", encoding="utf-8") as f:
        corps = json.load(f)

    for c in corps:
        db.add(WoningCorporatie(
            l_nummer=c["l_nummer"],
            naam=c["naam"],
            provincie=c.get("provincie", ""),
            grootte_klasse=c.get("grootte_klasse", ""),
            aantal_vhe=c.get("aantal_vhe", ""),
        ))

    db.commit()
    logger.info("Seeded %d woningcorporaties.", len(corps))


# ── WoCo sector categories ──────────────────────────────────────────
def seed_woco_categories(db: Session) -> None:
    """Seed the 119 WoCo sector-specific categories."""
    count = db.query(InkoopCategory).filter(
        InkoopCategory.category_system == "woco"
    ).count()
    if count > 0:
        logger.info("woco categories already seeded (%d rows) – skipping.", count)
        return

    if not WOCO_JSON_PATH.exists():
        logger.warning("No WoCo categories seed data at '%s'", WOCO_JSON_PATH)
        return

    with open(WOCO_JSON_PATH, "r", encoding="utf-8") as f:
        categories = json.load(f)

    for cat in categories:
        db.add(InkoopCategory(
            category_system="woco",
            groep=cat.get("groep", ""),
            sector=None,
            nummer=cat.get("nummer", ""),
            inkooppakket=cat.get("inkooppakket", ""),
            definitie=None,
            soort_inkoop="",
            classificatie=cat.get("classificatie") or None,
        ))

    db.commit()
    logger.info("Seeded %d WoCo sector categories.", len(categories))


# ── Leveranciers (supplier master) ──────────────────────────────────
def seed_leveranciers(db: Session) -> None:
    """Seed 1179 known suppliers into the supplier master database."""
    if not LEVERANCIERS_JSON_PATH.exists():
        logger.warning("No leveranciers seed data at '%s'", LEVERANCIERS_JSON_PATH)
        return

    existing = db.query(SupplierMasterCategory).filter(
        SupplierMasterCategory.source == "seed"
    ).count()
    if existing > 0:
        logger.info("Leveranciers already seeded (%d rows) – skipping.", existing)
        return

    with open(LEVERANCIERS_JSON_PATH, "r", encoding="utf-8") as f:
        leveranciers = json.load(f)

    inserted = 0
    for lev in leveranciers:
        naam = lev["naam"].strip()
        if not naam:
            continue
        normalized = _normalize_name(naam)
        exists = db.query(SupplierMasterCategory).filter(
            SupplierMasterCategory.normalized_name == normalized,
        ).first()
        if exists:
            continue

        db.add(SupplierMasterCategory(
            normalized_name=normalized,
            category_system="woco",
            display_name=naam,
            category_id=None,
            category_nummer="",
            category_name="",
            usage_count=1,
            source="seed",
            notes=f"KvK: {lev.get('kvk_nummer', '')}" if lev.get("kvk_nummer") else None,
        ))
        inserted += 1

    db.commit()
    logger.info("Seeded %d leveranciers into supplier master.", inserted)


# ── Backfill UserOrganization for existing data ─────────────────────
def seed_user_organizations(db: Session) -> None:
    """Ensure every Organization has at least one UserOrganization row."""
    try:
        from sqlalchemy import text
        rows = db.execute(text(
            "SELECT o.id, o.created_by FROM organizations o "
            "WHERE o.id NOT IN (SELECT organization_id FROM user_organizations)"
        )).fetchall()
    except Exception as e:
        logger.warning("seed_user_organizations skipped: %s", e)
        db.rollback()
        return

    if not rows:
        return

    for org_id, created_by in rows:
        db.add(UserOrganization(
            user_id=created_by,
            organization_id=org_id,
            role="eigenaar",
        ))
    db.commit()
    logger.info("Backfilled UserOrganization for %d existing organizations.", len(rows))


# ── Ensure at least one platform eigenaar ────────────────────────────
def seed_platform_eigenaar(db: Session) -> None:
    """If no user has platform_role set, promote the first user."""
    try:
        from sqlalchemy import text
        has_platform_user = db.execute(
            text("SELECT 1 FROM users WHERE platform_role IS NOT NULL LIMIT 1")
        ).first()
        if has_platform_user:
            return
        first_user = db.execute(
            text("SELECT id, email FROM users ORDER BY id ASC LIMIT 1")
        ).first()
        if first_user:
            db.execute(
                text("UPDATE users SET platform_role = 'eigenaar' WHERE id = :uid"),
                {"uid": first_user[0]},
            )
            db.commit()
            logger.info("Promoted user '%s' (id=%d) to platform eigenaar.", first_user[1], first_user[0])
    except Exception as e:
        logger.warning("seed_platform_eigenaar skipped: %s", e)
        db.rollback()


# ── PIANOo categories (legacy) ──────────────────────────────────────
def seed_inkoop_categories(db: Session) -> None:
    """Seed Aedes and BU WoCo categories from JSON files."""
    count = db.query(InkoopCategory).filter(
        InkoopCategory.category_system == "aedes"
    ).count()
    if count > 0:
        logger.info("inkoop_categories already has %d Aedes rows – skipping seed.", count)
    else:
        if AEDES_JSON_PATH.exists():
            _seed_from_json(db, AEDES_JSON_PATH, "aedes")

    bu_woco_count = db.query(InkoopCategory).filter(
        InkoopCategory.category_system == "bu_woco"
    ).count()
    if bu_woco_count == 0 and BU_WOCO_JSON_PATH.exists():
        _seed_from_json(db, BU_WOCO_JSON_PATH, "bu_woco")

    # Always seed WoCo categories
    seed_woco_categories(db)


def _seed_from_json(db: Session, json_path: Path, category_system: str = "aedes") -> None:
    """Seed categories from a JSON file for a specific category system."""
    with open(json_path, "r", encoding="utf-8") as f:
        categories = json.load(f)

    inserted = 0
    for cat in categories:
        db.add(InkoopCategory(
            category_system=category_system,
            groep=cat.get("groep", ""),
            sector=cat.get("sector"),
            nummer=cat.get("nummer", ""),
            inkooppakket=cat.get("inkooppakket", ""),
            definitie=cat.get("definitie"),
            soort_inkoop=cat.get("soort_inkoop", ""),
            cpv_code=cat.get("cpv_code"),
            homogeen=cat.get("homogeen"),
        ))
        inserted += 1

    db.commit()
    logger.info("Inserted %d %s categories from JSON.", inserted, category_system)
