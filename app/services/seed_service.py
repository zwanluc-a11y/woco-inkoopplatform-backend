"""
Seed helpers – called once at application startup.

* seed_inkoop_categories – imports the PIANOo inkooppakketten Excel or JSON
  seed (if the table is still empty)
* seed_user_organizations – backfills UserOrganization for existing data
"""

import json
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

from app.config import settings
from app.models.category import InkoopCategory
from app.models.user_organization import UserOrganization

logger = logging.getLogger(__name__)

# JSON seed files bundled with the app
_SEED_DIR = Path(__file__).resolve().parent.parent.parent / "data"
AEDES_JSON_PATH = _SEED_DIR / "categories" / "aedes_categories.json"
BU_WOCO_JSON_PATH = _SEED_DIR / "categories" / "bu_woco_categories.json"


# ── Backfill UserOrganization for existing data ──────────────────────
def seed_user_organizations(db: Session) -> None:
    """Ensure every Organization has at least one UserOrganization row.

    This is a one-time migration for existing data created before
    the multi-tenancy feature was introduced.

    Uses raw SQL to avoid issues when ORM model has columns that
    don't exist in the database yet (e.g. during first migration).
    """
    try:
        from sqlalchemy import text
        rows = db.execute(text(
            "SELECT o.id, o.created_by FROM organizations o "
            "WHERE o.id NOT IN (SELECT organization_id FROM user_organizations)"
        )).fetchall()
    except Exception as e:
        logger.warning("seed_user_organizations skipped (table may not exist yet): %s", e)
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
    logger.info(
        "Backfilled UserOrganization for %d existing organizations.",
        len(rows),
    )


# ── Ensure at least one platform eigenaar ─────────────────────────────
def seed_platform_eigenaar(db: Session) -> None:
    """Ensure at least one platform eigenaar exists.

    If no user has platform_role set, promote the first user (by id).
    """
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
            logger.info(
                "Promoted user '%s' (id=%d) to platform eigenaar (migration).",
                first_user[1], first_user[0],
            )
    except Exception as e:
        logger.warning("seed_platform_eigenaar skipped: %s", e)
        db.rollback()


# ── PIANOo categories ────────────────────────────────────────────────
def seed_inkoop_categories(db: Session) -> None:
    """Read the PIANOo Excel file and populate the inkoop_categories table.

    The function is a no-op when PIANOo categories already exist, so it is
    safe to call on every startup.
    """
    count = db.query(InkoopCategory).filter(
        InkoopCategory.category_system == "aedes"
    ).count()
    if count > 0:
        logger.info(
            "inkoop_categories table already has %d PIANOo rows – skipping seed.", count
        )
        return

    # Seed Aedes categories
    if AEDES_JSON_PATH.exists():
        logger.info("Seeding Aedes categories from JSON '%s' ...", AEDES_JSON_PATH)
        _seed_from_json(db, AEDES_JSON_PATH, "aedes")
    else:
        logger.warning("No Aedes seed data found at '%s'", AEDES_JSON_PATH)

    # Seed BU WoCo categories
    bu_woco_count = db.query(InkoopCategory).filter(
        InkoopCategory.category_system == "bu_woco"
    ).count()
    if bu_woco_count == 0 and BU_WOCO_JSON_PATH.exists():
        logger.info("Seeding BU WoCo categories from JSON '%s' ...", BU_WOCO_JSON_PATH)
        _seed_from_json(db, BU_WOCO_JSON_PATH, "bu_woco")

    return

    logger.info("Reading PIANOo categories from '%s' …", excel_path)
    df = pd.read_excel(excel_path)

    # ── Forward-fill the 'Groep' column ───────────────────────────────
    df["Groep"] = df["Groep"].ffill()

    # ── Drop separator / blank rows (no Inkooppakket value) ───────────
    df = df.dropna(subset=["Inkooppakket"])

    # ── Map column names to model fields ──────────────────────────────
    cpv_col = [c for c in df.columns if c.lower().startswith("cpv")]
    cpv_col = cpv_col[0] if cpv_col else None

    inserted = 0
    for _, row in df.iterrows():
        homogeen_raw = row.get("Homogeen")
        if pd.isna(homogeen_raw):
            homogeen = None
        else:
            homogeen = str(homogeen_raw).strip().lower() == "ja"

        nummer_raw = row.get("Nummer")
        if pd.isna(nummer_raw):
            continue
        nummer = str(int(nummer_raw)) if isinstance(nummer_raw, float) else str(nummer_raw)

        soort_col = [c for c in df.columns if "soort inkoop" in c.lower() and "nieuw" in c.lower()]
        soort_inkoop = ""
        if soort_col:
            soort_val = row.get(soort_col[0])
            soort_inkoop = str(soort_val).strip() if not pd.isna(soort_val) else ""

        cpv_value = None
        if cpv_col:
            raw = row.get(cpv_col)
            cpv_value = str(raw).strip() if not pd.isna(raw) else None

        definitie_raw = row.get("Definitie / voorbeelden")
        if definitie_raw is None or pd.isna(definitie_raw):
            def_cols = [c for c in df.columns if c.lower().startswith("definitie")]
            if def_cols:
                definitie_raw = row.get(def_cols[0])

        definitie = str(definitie_raw).strip() if definitie_raw is not None and not pd.isna(definitie_raw) else None

        category = InkoopCategory(
            groep=str(row["Groep"]).strip(),
            sector=str(row["Sector"]).strip() if not pd.isna(row.get("Sector")) else None,
            nummer=nummer,
            inkooppakket=str(row["Inkooppakket"]).strip(),
            definitie=definitie,
            soort_inkoop=soort_inkoop,
            cpv_code=cpv_value,
            homogeen=homogeen,
        )
        db.add(category)
        inserted += 1

    db.commit()
    logger.info("Inserted %d PIANOo categories.", inserted)


def _seed_from_json(db: Session, json_path: Path, category_system: str = "aedes") -> None:
    """Seed categories from a JSON file for a specific category system."""
    with open(json_path, "r", encoding="utf-8") as f:
        categories = json.load(f)

    inserted = 0
    for cat in categories:
        category = InkoopCategory(
            category_system=category_system,
            groep=cat.get("groep", ""),
            sector=cat.get("sector"),
            nummer=cat.get("nummer", ""),
            inkooppakket=cat.get("inkooppakket", ""),
            definitie=cat.get("definitie"),
            soort_inkoop=cat.get("soort_inkoop", ""),
            cpv_code=cat.get("cpv_code"),
            homogeen=cat.get("homogeen"),
        )
        db.add(category)
        inserted += 1

    db.commit()
    logger.info("Inserted %d %s categories from JSON.", inserted, category_system)


