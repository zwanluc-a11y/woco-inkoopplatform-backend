"""
Risk Assessment Service.

Calculates procurement risk per PIANOo category based on:
- Aggregated yearly spend across all suppliers in the category
- Expected contract duration (configurable, default 4 years)
- Organization-specific procurement thresholds
- Existing contract coverage
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.category_duration_setting import CategoryDurationSetting
from app.models.contract import Contract, ContractSupplier
from app.models.organization import Organization
from app.models.category import InkoopCategory
from app.models.risk_assessment import RiskAssessment
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_yearly_spend import SupplierYearlySpend
from app.models.threshold import Threshold

logger = logging.getLogger(__name__)


class RiskService:

    def __init__(self, db: Session):
        self.db = db

    def get_internal_threshold(self, org_id: int, year: int) -> Optional[Threshold]:
        """Get the correct threshold for an organization and year."""
        # Determine threshold period based on year
        if year <= 2025:
            period = "2024-2025"
        else:
            period = "2026-2027"

        threshold = (
            self.db.query(Threshold)
            .filter(
                Threshold.organization_id == org_id,
                Threshold.threshold_period == period,
            )
            .first()
        )

        return threshold

    def get_threshold_for_type(self, threshold: Threshold, soort_inkoop: str | None) -> float:
        """Get the specific threshold amount for a procurement type."""
        soort = (soort_inkoop or "").lower().strip()
        if not soort:
            # Default for categories without soort_inkoop (e.g. Aedes/Trevian)
            return float(threshold.diensten_leveringen)
        if "werk" in soort:
            return float(threshold.werken)
        elif "social" in soort or "bijlage" in soort or "specifiek" in soort:
            return float(threshold.ict_diensten)
        elif "niet" in soort and "aanbesteding" in soort:
            return 999_999_999  # Effectively no threshold
        else:
            # Diensten, Leveringen, and everything else
            return float(threshold.diensten_leveringen)

    def get_duration_for_category(self, org_id: int, category_id: int) -> float:
        """Get expected contract duration for a category (default 4 years)."""
        setting = (
            self.db.query(CategoryDurationSetting)
            .filter(
                CategoryDurationSetting.organization_id == org_id,
                CategoryDurationSetting.category_id == category_id,
            )
            .first()
        )

        return setting.expected_duration_years if setting else 4.0

    def has_active_contract(self, org_id: int, category_id: int) -> tuple:
        """Check if there's an active aanbesteed contract covering this category.

        Checks two paths:
        1. Direct link: Contract.category_id == category_id
        2. Indirect link: Contract → suppliers → SupplierCategorization → category
        Returns (has_contract, end_date, contract_name, contract_id).
        """
        from sqlalchemy import or_

        # Path 1: Direct category link
        direct_contract = (
            self.db.query(Contract)
            .filter(
                Contract.organization_id == org_id,
                Contract.is_ingekocht_via_procedure.is_(True),
                Contract.category_id == category_id,
                Contract.status.in_(["active", "expiring"]),
            )
            .first()
        )

        if direct_contract:
            end_date = direct_contract.max_end_date or direct_contract.end_date
            return True, end_date, direct_contract.name, direct_contract.id

        # Path 2: Indirect via supplier overlap
        supplier_ids = [
            s.supplier_id
            for s in self.db.query(SupplierCategorization.supplier_id)
            .filter(
                SupplierCategorization.organization_id == org_id,
                SupplierCategorization.category_id == category_id,
            )
            .all()
        ]

        if not supplier_ids:
            return False, None, None, None

        contract = (
            self.db.query(Contract)
            .join(ContractSupplier)
            .filter(
                Contract.organization_id == org_id,
                Contract.is_ingekocht_via_procedure.is_(True),
                ContractSupplier.supplier_id.in_(supplier_ids),
                Contract.status.in_(["active", "expiring"]),
            )
            .first()
        )

        if contract:
            end_date = contract.max_end_date or contract.end_date
            return True, end_date, contract.name, contract.id

        return False, None, None, None

    def get_available_years(self, org_id: int) -> list[int]:
        """Get years that have spend data for this organization."""
        years = (
            self.db.query(SupplierYearlySpend.year)
            .filter(SupplierYearlySpend.organization_id == org_id)
            .distinct()
            .order_by(SupplierYearlySpend.year.desc())
            .all()
        )
        return [y[0] for y in years]

    def calculate_risk(
        self, org_id: int, assessment_year: int, user_id: Optional[int] = None
    ) -> list[dict]:
        """
        Calculate risk assessment for all categorized PIANOo categories.

        Returns list of risk assessments with:
        - Category info
        - Yearly spend aggregate
        - Estimated contract value (spend x duration)
        - Applicable threshold
        - Risk level: akkoord, onderzoek, aanbesteden, monitoren
        """
        threshold = self.get_internal_threshold(org_id, assessment_year)
        if not threshold:
            raise ValueError(
                f"Geen drempels geconfigureerd voor organisatie {org_id}"
            )

        # Diagnostic: count total categorized suppliers and categories
        total_categorized_suppliers = (
            self.db.query(func.count(func.distinct(SupplierCategorization.supplier_id)))
            .filter(SupplierCategorization.organization_id == org_id)
            .scalar() or 0
        )
        total_categorized_categories = (
            self.db.query(func.count(func.distinct(SupplierCategorization.category_id)))
            .filter(SupplierCategorization.organization_id == org_id)
            .scalar() or 0
        )
        # Count suppliers with spend data for this year
        suppliers_with_spend = (
            self.db.query(func.count(func.distinct(SupplierYearlySpend.supplier_id)))
            .filter(
                SupplierYearlySpend.organization_id == org_id,
                SupplierYearlySpend.year == assessment_year,
            )
            .scalar() or 0
        )
        # Count categorized suppliers that also have spend data
        categorized_with_spend = (
            self.db.query(func.count(func.distinct(SupplierCategorization.supplier_id)))
            .join(
                SupplierYearlySpend,
                (SupplierYearlySpend.supplier_id == SupplierCategorization.supplier_id)
                & (SupplierYearlySpend.year == assessment_year),
            )
            .filter(SupplierCategorization.organization_id == org_id)
            .scalar() or 0
        )

        # Get all categorized spend by PIANOo category for the assessment year (percentage-weighted)
        category_spends = (
            self.db.query(
                SupplierCategorization.category_id,
                func.sum(SupplierYearlySpend.total_amount * SupplierCategorization.percentage / 100.0).label("yearly_spend"),
                func.count(func.distinct(Supplier.id)).label("supplier_count"),
            )
            .join(Supplier, SupplierCategorization.supplier_id == Supplier.id)
            .join(
                SupplierYearlySpend,
                (SupplierYearlySpend.supplier_id == Supplier.id)
                & (SupplierYearlySpend.year == assessment_year),
            )
            .filter(SupplierCategorization.organization_id == org_id)
            .group_by(SupplierCategorization.category_id)
            .all()
        )

        # Delete old assessments for this org + year
        self.db.query(RiskAssessment).filter(
            RiskAssessment.organization_id == org_id,
            RiskAssessment.assessment_year == assessment_year,
        ).delete()

        results = []

        for cat_id, yearly_spend, supplier_count in category_spends:
            category = self.db.query(InkoopCategory).get(cat_id)
            if not category:
                continue

            yearly_spend = float(yearly_spend or 0)
            if yearly_spend <= 0:
                continue

            # Get duration and calculate estimated contract value
            duration = self.get_duration_for_category(org_id, cat_id)
            estimated_value = yearly_spend * duration

            # Get applicable threshold for this procurement type
            internal_threshold = self.get_threshold_for_type(
                threshold, category.soort_inkoop
            )

            # Check contract coverage
            has_contract, contract_end, contract_name, contract_id = self.has_active_contract(org_id, cat_id)

            # Determine risk level
            pct_of_threshold = (
                (estimated_value / internal_threshold * 100)
                if internal_threshold > 0
                else 0
            )

            if internal_threshold >= 999_999_999:
                risk_level = "vrije_inkoop"  # Not subject to procurement law
            elif has_contract and pct_of_threshold > 100:
                risk_level = "enkelvoudig_onderhands"  # Above threshold but covered by contract
            elif pct_of_threshold > 100:
                risk_level = "offertetraject"  # Above threshold, no contract
            elif pct_of_threshold > 75:
                risk_level = "meervoudig_onderhands"  # Approaching threshold
            else:
                risk_level = "vrije_inkoop"  # Below threshold

            # Special: contract expiring within 12 months? Flag even if akkoord
            notes_parts: list[str] = []
            if contract_end:
                days_remaining = (contract_end - date.today()).days
                if days_remaining < 365:
                    notes_parts.append(
                        f"Contract loopt af op {contract_end.isoformat()} "
                        f"({days_remaining} dagen)"
                    )
                    if risk_level == "vrije_inkoop":
                        risk_level = "meervoudig_onderhands"

            if supplier_count > 3 and not has_contract:
                notes_parts.append(
                    f"{supplier_count} leveranciers zonder raamcontract "
                    "\u2014 mogelijke splitsing"
                )

            # Create assessment record
            assessment = RiskAssessment(
                organization_id=org_id,
                category_id=cat_id,
                assessment_year=assessment_year,
                yearly_spend=yearly_spend,
                supplier_count=supplier_count,
                duration_years=duration,
                estimated_contract_value=estimated_value,
                internal_threshold=internal_threshold,
                threshold_type=category.soort_inkoop or "Onbekend",
                risk_level=risk_level,
                has_contract=has_contract,
                notes="; ".join(notes_parts) if notes_parts else None,
                assessed_by=user_id,
            )
            self.db.add(assessment)

            results.append(
                {
                    "id": None,  # Will be set after commit
                    "category_id": cat_id,
                    "category_naam": category.inkooppakket,
                    "category_nummer": category.nummer,
                    "groep": category.groep,
                    "soort_inkoop": category.soort_inkoop,
                    "jaarlijkse_spend": yearly_spend,
                    "leverancier_count": supplier_count,
                    "verwachte_looptijd": duration,
                    "geraamde_opdrachtwaarde": estimated_value,
                    "toepasselijke_drempel": internal_threshold,
                    "percentage_van_drempel": round(pct_of_threshold, 1),
                    "risk_level": risk_level,
                    "has_contract": has_contract,
                    "contract_naam": contract_name,
                    "contract_id": contract_id,
                    "contract_end_date": (
                        contract_end.isoformat() if contract_end else None
                    ),
                    "notes": "; ".join(notes_parts) if notes_parts else None,
                }
            )

        self.db.commit()

        # Sort by risk severity, then by estimated value
        risk_order = {"offertetraject": 0, "meervoudig_onderhands": 1, "enkelvoudig_onderhands": 2, "vrije_inkoop": 3}
        results.sort(
            key=lambda r: (
                risk_order.get(r["risk_level"], 4),
                -r["geraamde_opdrachtwaarde"],
            )
        )

        return {
            "results": results,
            "diagnostics": {
                "total_categorized_suppliers": total_categorized_suppliers,
                "total_categorized_categories": total_categorized_categories,
                "suppliers_with_spend": suppliers_with_spend,
                "categorized_with_spend": categorized_with_spend,
                "categories_with_spend": len(results),
            },
        }

    def get_latest_assessments(self, org_id: int) -> list:
        """Get most recent risk assessments for an organization."""
        assessments = (
            self.db.query(RiskAssessment)
            .filter(RiskAssessment.organization_id == org_id)
            .order_by(RiskAssessment.assessed_at.desc())
            .all()
        )
        return assessments

    def get_risk_summary(self, org_id: int) -> dict:
        """Get aggregated risk summary matching frontend RiskSummary interface."""
        assessments = self.get_latest_assessments(org_id)

        # Frontend expects: { aanbesteden: {count, total_value}, ... }
        buckets = {
            "offertetraject": {"count": 0, "total_value": 0.0},
            "meervoudig_onderhands": {"count": 0, "total_value": 0.0},
            "enkelvoudig_onderhands": {"count": 0, "total_value": 0.0},
            "vrije_inkoop": {"count": 0, "total_value": 0.0},
        }

        for a in assessments:
            level = a.risk_level
            if level in buckets:
                buckets[level]["count"] += 1
                buckets[level]["total_value"] += float(a.estimated_contract_value)

        return buckets
