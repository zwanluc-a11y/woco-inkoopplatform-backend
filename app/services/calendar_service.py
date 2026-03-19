from __future__ import annotations

"""
Calendar Service

Generates procurement calendar items from:
1. Risk assessments with risk_level "offertetraject" (must procure)
2. Expiring contracts that need re-procurement
3. Risk assessments with risk_level "meervoudig_onderhands" (should investigate)
"""

import logging
from datetime import date, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from app.models.contract import Contract
from app.models.procurement_calendar_item import ProcurementCalendarItem
from app.models.procurement_calendar_phase import ProcurementCalendarPhase
from app.models.risk_assessment import RiskAssessment

logger = logging.getLogger(__name__)

# Default phase definitions with standard durations in weeks
DEFAULT_PHASES = [
    {"name": "Behoeftebepaling", "order": 1, "weeks": 6},
    {"name": "Marktconsultatie", "order": 2, "weeks": 4},
    {"name": "Publicatie", "order": 3, "weeks": 8},
    {"name": "Gunning", "order": 4, "weeks": 4},
    {"name": "Implementatie", "order": 5, "weeks": 4},
]


class CalendarService:
    def __init__(self, db: Session):
        self.db = db

    def generate_calendar(
        self,
        org_id: int,
        assessment_year: int,
    ) -> list[dict]:
        """Generate calendar items from risk assessments and expiring contracts."""
        # Clear existing generated items for this org
        self.db.query(ProcurementCalendarItem).filter(
            ProcurementCalendarItem.organization_id == org_id,
        ).delete()
        self.db.flush()

        items = []

        # 1. Items from risk assessments that require procurement
        risk_items = (
            self.db.query(RiskAssessment)
            .filter(
                RiskAssessment.organization_id == org_id,
                RiskAssessment.assessment_year == assessment_year,
                RiskAssessment.risk_level.in_(["offertetraject", "meervoudig_onderhands"]),
            )
            .all()
        )

        today = date.today()
        for ra in risk_items:
            # Determine priority based on risk level and spend
            if ra.risk_level == "offertetraject":
                priority = "high"
                if ra.estimated_contract_value > ra.internal_threshold * 1.5:
                    priority = "high"
            else:
                priority = "medium"

            # Calculate target dates
            # Preparation period: ~6 months for high priority, ~9 months for medium
            prep_months = 6 if priority == "high" else 9
            target_start = today + timedelta(days=30)  # Start prep in 1 month
            target_publish = target_start + timedelta(days=prep_months * 30)

            cat_name = ""
            if ra.category:
                cat_name = ra.category.inkooppakket

            title = f"Aanbesteding {cat_name}" if ra.risk_level == "offertetraject" else f"Onderzoek {cat_name}"
            desc_parts = [
                f"Jaarlijkse spend: €{ra.yearly_spend:,.0f}",
                f"Geraamde opdrachtwaarde: €{ra.estimated_contract_value:,.0f}",
                f"Drempel: €{ra.internal_threshold:,.0f}",
                f"Risico: {ra.risk_level}",
            ]
            if ra.notes:
                desc_parts.append(ra.notes)

            item = ProcurementCalendarItem(
                organization_id=org_id,
                risk_assessment_id=ra.id,
                category_id=ra.category_id,
                title=title,
                description="\n".join(desc_parts),
                priority=priority,
                target_start_date=target_start,
                target_publish_date=target_publish,
                estimated_value=ra.estimated_contract_value,
                status="planned",
            )
            self.db.add(item)
            items.append(item)

        # Create phases for risk-based items
        self.db.flush()
        for item in items:
            self._create_default_phases(item)

        # 2. Items from expiring contracts (within 18 months)
        expiry_cutoff = today + timedelta(days=18 * 30)
        expiring_contracts = (
            self.db.query(Contract)
            .filter(
                Contract.organization_id == org_id,
                Contract.end_date != None,
                Contract.end_date <= expiry_cutoff,
                Contract.end_date >= today,
            )
            .all()
        )

        for contract in expiring_contracts:
            # Check if there's already a calendar item for this contract's category
            # via risk assessment
            existing = any(
                i.contract_id == contract.id for i in items
            )
            if existing:
                continue

            days_until_expiry = (contract.end_date - today).days
            if days_until_expiry <= 180:
                priority = "high"
            elif days_until_expiry <= 365:
                priority = "medium"
            else:
                priority = "low"

            # Target: start prep 6 months before contract ends
            target_start = contract.end_date - timedelta(days=180)
            if target_start < today:
                target_start = today + timedelta(days=14)
            target_publish = contract.end_date - timedelta(days=60)

            desc_parts = [
                f"Contract: {contract.name}",
                f"Einddatum: {contract.end_date.isoformat()}",
            ]
            if contract.extension_options:
                desc_parts.append(f"Verlengingsopties: {contract.extension_options}")
            if contract.max_end_date:
                desc_parts.append(f"Maximale einddatum: {contract.max_end_date.isoformat()}")
            if contract.estimated_value:
                desc_parts.append(f"Contractwaarde: €{contract.estimated_value:,.0f}")

            item = ProcurementCalendarItem(
                organization_id=org_id,
                contract_id=contract.id,
                title=f"Heraanbesteding {contract.name}",
                description="\n".join(desc_parts),
                priority=priority,
                target_start_date=target_start,
                target_publish_date=target_publish,
                estimated_value=contract.estimated_value,
                status="planned",
            )
            self.db.add(item)
            items.append(item)

        # Create phases for contract-based items
        self.db.flush()
        for item in items:
            if not item.phases:
                self._create_default_phases(item)

        self.db.commit()

        # Refresh items to load phases
        for item in items:
            self.db.refresh(item)

        logger.info(
            "Generated %d calendar items for org %d", len(items), org_id
        )

        return [self._item_to_dict(i) for i in items]

    def get_calendar(self, org_id: int) -> list[dict]:
        """Get all calendar items for an organization."""
        items = (
            self.db.query(ProcurementCalendarItem)
            .filter(ProcurementCalendarItem.organization_id == org_id)
            .order_by(
                ProcurementCalendarItem.priority.desc(),
                ProcurementCalendarItem.target_start_date.asc(),
            )
            .all()
        )
        return [self._item_to_dict(i) for i in items]

    def update_item(
        self,
        org_id: int,
        item_id: int,
        updates: dict,
    ) -> dict:
        """Update a calendar item."""
        item = (
            self.db.query(ProcurementCalendarItem)
            .filter(
                ProcurementCalendarItem.id == item_id,
                ProcurementCalendarItem.organization_id == org_id,
            )
            .first()
        )
        if not item:
            raise ValueError("Kalenderitem niet gevonden")

        for key, val in updates.items():
            if hasattr(item, key):
                setattr(item, key, val)

        self.db.commit()
        self.db.refresh(item)
        return self._item_to_dict(item)

    def _create_default_phases(self, item: ProcurementCalendarItem) -> None:
        """Create default procurement phases for a calendar item."""
        start = item.target_start_date or date.today() + timedelta(days=30)

        current_start = start
        for phase_def in DEFAULT_PHASES:
            phase_end = current_start + timedelta(weeks=phase_def["weeks"])
            phase = ProcurementCalendarPhase(
                calendar_item_id=item.id,
                phase_name=phase_def["name"],
                phase_order=phase_def["order"],
                status="niet_gestart",
                planned_start_date=current_start,
                planned_end_date=phase_end,
            )
            self.db.add(phase)
            current_start = phase_end

    def update_phase(
        self,
        org_id: int,
        item_id: int,
        phase_id: int,
        updates: dict,
    ) -> dict:
        """Update a calendar phase."""
        # Verify item belongs to org
        item = (
            self.db.query(ProcurementCalendarItem)
            .filter(
                ProcurementCalendarItem.id == item_id,
                ProcurementCalendarItem.organization_id == org_id,
            )
            .first()
        )
        if not item:
            raise ValueError("Kalenderitem niet gevonden")

        phase = (
            self.db.query(ProcurementCalendarPhase)
            .filter(
                ProcurementCalendarPhase.id == phase_id,
                ProcurementCalendarPhase.calendar_item_id == item_id,
            )
            .first()
        )
        if not phase:
            raise ValueError("Fase niet gevonden")

        for key, val in updates.items():
            if hasattr(phase, key):
                setattr(phase, key, val)

        self.db.commit()
        self.db.refresh(phase)
        return self._phase_to_dict(phase)

    def _phase_to_dict(self, phase: ProcurementCalendarPhase) -> dict:
        return {
            "id": phase.id,
            "calendar_item_id": phase.calendar_item_id,
            "phase_name": phase.phase_name,
            "phase_order": phase.phase_order,
            "status": phase.status,
            "planned_start_date": phase.planned_start_date.isoformat() if phase.planned_start_date else None,
            "planned_end_date": phase.planned_end_date.isoformat() if phase.planned_end_date else None,
            "notes": phase.notes,
            "created_at": phase.created_at.isoformat() if phase.created_at else None,
            "updated_at": phase.updated_at.isoformat() if phase.updated_at else None,
        }

    def _item_to_dict(self, item: ProcurementCalendarItem) -> dict:
        cat_name = None
        cat_groep = None
        if item.category:
            cat_name = item.category.inkooppakket
            cat_groep = item.category.groep
        contract_name = None
        if item.contract:
            contract_name = item.contract.name

        phases = []
        if item.phases:
            phases = [self._phase_to_dict(p) for p in item.phases]

        return {
            "id": item.id,
            "organization_id": item.organization_id,
            "risk_assessment_id": item.risk_assessment_id,
            "contract_id": item.contract_id,
            "category_id": item.category_id,
            "category_name": cat_name,
            "category_groep": cat_groep,
            "contract_name": contract_name,
            "title": item.title,
            "description": item.description,
            "priority": item.priority,
            "target_start_date": item.target_start_date.isoformat() if item.target_start_date else None,
            "target_publish_date": item.target_publish_date.isoformat() if item.target_publish_date else None,
            "estimated_value": float(item.estimated_value) if item.estimated_value else None,
            "status": item.status,
            "created_at": item.created_at.isoformat() if item.created_at else None,
            "phases": phases,
        }
