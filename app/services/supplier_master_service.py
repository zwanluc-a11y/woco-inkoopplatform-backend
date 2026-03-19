"""
Supplier Master Database Service.

Cross-organization knowledge base of supplier → PIANOo category mappings.
Auto-populated from confirmed categorizations, used for suggestions in new orgs.
"""
from __future__ import annotations

import io
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.category import InkoopCategory
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_master_category import SupplierMasterCategory
from app.services.import_service import normalize_supplier_name

logger = logging.getLogger(__name__)


class SupplierMasterService:
    def __init__(self, db: Session):
        self.db = db

    # ── Core CRUD ───────────────────────────────────────────────────

    def upsert(
        self,
        normalized_name: str,
        display_name: str,
        category_id: int,
        category_nummer: str,
        category_name: str,
        source: str = "auto",
        category_system: str = "aedes",
    ) -> SupplierMasterCategory:
        """Add or update a supplier-category mapping. Increments usage_count if exists."""
        existing = (
            self.db.query(SupplierMasterCategory)
            .filter(
                SupplierMasterCategory.normalized_name == normalized_name,
                SupplierMasterCategory.category_id == category_id,
                SupplierMasterCategory.category_system == "aedes",
            )
            .first()
        )
        if existing:
            existing.usage_count += 1
            existing.updated_at = datetime.utcnow()
            if source == "manual":
                existing.source = "manual"
            return existing
        else:
            entry = SupplierMasterCategory(
                normalized_name=normalized_name,
                display_name=display_name,
                category_id=category_id,
                category_nummer=category_nummer,
                category_name=category_name,
                category_system="aedes",
                usage_count=1,
                source=source,
            )
            self.db.add(entry)
            return entry

    def record_categorization(
        self, supplier: Supplier, categorization: SupplierCategorization
    ) -> None:
        """Called after a categorization is confirmed. Upserts into master DB."""
        if categorization.source not in (
            "manual", "ai_accepted", "ai_confirmed", "imported",
        ):
            return

        category = self.db.query(InkoopCategory).get(
            categorization.category_id
        )
        if not category:
            return

        self.upsert(
            normalized_name=supplier.normalized_name,
            display_name=supplier.name,
            category_id=category.id,
            category_nummer=category.nummer,
            category_name=category.inkooppakket,
            source="auto",
        )

    # ── Lookup ──────────────────────────────────────────────────────

    def lookup(
        self, normalized_name: str, category_system: str | None = None
    ) -> list[SupplierMasterCategory]:
        """Find all category mappings for a normalized supplier name."""
        q = (
            self.db.query(SupplierMasterCategory)
            .filter(
                SupplierMasterCategory.normalized_name == normalized_name,
                SupplierMasterCategory.category_system == "aedes",
            )
        )
        return q.order_by(SupplierMasterCategory.usage_count.desc()).all()

    def bulk_lookup(
        self, normalized_names: list[str], category_system: str = "aedes"
    ) -> dict[str, list[SupplierMasterCategory]]:
        """Lookup multiple supplier names at once (efficient IN-clause)."""
        if not normalized_names:
            return {}
        entries = (
            self.db.query(SupplierMasterCategory)
            .filter(
                SupplierMasterCategory.normalized_name.in_(normalized_names),
                SupplierMasterCategory.category_system == "aedes",
            )
            .order_by(SupplierMasterCategory.usage_count.desc())
            .all()
        )
        result: dict[str, list[SupplierMasterCategory]] = {}
        for e in entries:
            result.setdefault(e.normalized_name, []).append(e)
        return result

    # ── Search & List ───────────────────────────────────────────────

    def search(
        self,
        query: str = "",
        page: int = 1,
        page_size: int = 50,
        category_system: str | None = None,
    ) -> tuple[list[SupplierMasterCategory], int]:
        """Search master DB by supplier name or category name."""
        q = self.db.query(SupplierMasterCategory).filter(
            SupplierMasterCategory.category_system == "aedes"
        )
        if query:
            pattern = f"%{query}%"
            q = q.filter(
                SupplierMasterCategory.display_name.ilike(pattern)
                | SupplierMasterCategory.category_name.ilike(pattern)
                | SupplierMasterCategory.category_nummer.ilike(pattern)
            )
        total = q.count()
        offset = (page - 1) * page_size
        entries = (
            q.order_by(
                SupplierMasterCategory.display_name,
                SupplierMasterCategory.category_nummer,
            )
            .offset(offset)
            .limit(page_size)
            .all()
        )
        return entries, total

    def get_stats(self, category_system: str | None = None) -> dict:
        """Get aggregate statistics for the master DB."""
        base = self.db.query(SupplierMasterCategory).filter(
            SupplierMasterCategory.category_system == "aedes"
        )

        total_entries = base.count()
        unique_suppliers = (
            base.with_entities(
                func.count(func.distinct(SupplierMasterCategory.normalized_name))
            ).scalar()
            or 0
        )
        top_q = (
            base.with_entities(
                SupplierMasterCategory.category_name,
                func.count(SupplierMasterCategory.id).label("cnt"),
            )
            .group_by(SupplierMasterCategory.category_name)
            .order_by(func.count(SupplierMasterCategory.id).desc())
        )
        top_category = top_q.first()
        return {
            "total_entries": total_entries,
            "unique_suppliers": unique_suppliers,
            "top_category": top_category[0] if top_category else None,
            "top_category_count": top_category[1] if top_category else 0,
        }

    # ── Update & Delete ─────────────────────────────────────────────

    def update_entry(
        self,
        entry_id: int,
        category_id: Optional[int] = None,
        notes: Optional[str] = None,
        display_name: Optional[str] = None,
    ) -> Optional[SupplierMasterCategory]:
        """Update an existing master entry."""
        entry = self.db.query(SupplierMasterCategory).get(entry_id)
        if not entry:
            return None

        if category_id is not None and category_id != entry.category_id:
            category = self.db.query(InkoopCategory).get(category_id)
            if category:
                entry.category_id = category.id
                entry.category_nummer = category.nummer
                entry.category_name = category.inkooppakket

        if notes is not None:
            entry.notes = notes
        if display_name is not None:
            entry.display_name = display_name
            entry.normalized_name = normalize_supplier_name(display_name)

        entry.updated_at = datetime.utcnow()
        return entry

    def delete_entry(self, entry_id: int) -> bool:
        """Delete a single master entry."""
        entry = self.db.query(SupplierMasterCategory).get(entry_id)
        if not entry:
            return False
        self.db.delete(entry)
        return True

    # ── CSV Import ──────────────────────────────────────────────────

    def bulk_upsert_from_csv(
        self, file_bytes: bytes, category_system: str = "aedes"
    ) -> dict:
        """
        Process a CSV with supplier_name and category_nummer columns.
        Returns stats: {created, updated, skipped, errors}.
        """
        try:
            df = pd.read_csv(io.BytesIO(file_bytes))
        except Exception as e:
            return {"created": 0, "updated": 0, "skipped": 0, "errors": [str(e)]}

        # Find columns (case-insensitive)
        col_map: dict[str, str] = {}
        for col in df.columns:
            lower = col.strip().lower()
            if "supplier" in lower or "leverancier" in lower or "naam" in lower:
                col_map["name"] = col
            elif "nummer" in lower or "number" in lower or "code" in lower:
                col_map["nummer"] = col
            elif "notes" in lower or "notities" in lower or "opmerking" in lower:
                col_map["notes"] = col

        if "name" not in col_map or "nummer" not in col_map:
            return {
                "created": 0,
                "updated": 0,
                "skipped": 0,
                "errors": [
                    "CSV moet kolommen bevatten voor leveranciersnaam "
                    "(supplier_name/leverancier) en categorienummer "
                    "(category_nummer/nummer)."
                ],
            }

        # Build InkoopCategory lookup by nummer (PIANOo only)
        categories = (
            self.db.query(InkoopCategory)
            .filter(InkoopCategory.category_system == "aedes")
            .all()
        )
        cat_by_nummer = {c.nummer.strip(): c for c in categories}

        created = 0
        updated = 0
        skipped = 0
        errors: list[str] = []

        for idx, row in df.iterrows():
            name_raw = str(row[col_map["name"]]).strip()
            nummer_raw = str(row[col_map["nummer"]]).strip()
            notes_raw = str(row.get(col_map.get("notes", ""), "")).strip()
            notes_val = notes_raw if notes_raw and notes_raw != "nan" else None

            if not name_raw or name_raw == "nan" or not nummer_raw or nummer_raw == "nan":
                skipped += 1
                continue

            category = cat_by_nummer.get(nummer_raw)
            if not category:
                errors.append(f"Rij {idx + 2}: categorie '{nummer_raw}' niet gevonden")
                continue

            normalized = normalize_supplier_name(name_raw)
            existing = (
                self.db.query(SupplierMasterCategory)
                .filter(
                    SupplierMasterCategory.normalized_name == normalized,
                    SupplierMasterCategory.category_id == category.id,
                    SupplierMasterCategory.category_system == "aedes",
                )
                .first()
            )

            if existing:
                existing.usage_count += 1
                existing.updated_at = datetime.utcnow()
                if notes_val:
                    existing.notes = notes_val
                updated += 1
            else:
                entry = SupplierMasterCategory(
                    normalized_name=normalized,
                    display_name=name_raw,
                    category_id=category.id,
                    category_nummer=category.nummer,
                    category_name=category.inkooppakket,
                    category_system="aedes",
                    usage_count=1,
                    source="imported",
                    notes=notes_val,
                )
                self.db.add(entry)
                created += 1

        self.db.commit()
        return {
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "errors": errors,
        }
