"""
AI Categorization Service using Claude API.

Analyzes supplier names and invoice descriptions to suggest
PIANOo procurement categories.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.config import settings
from app.models.category import InkoopCategory
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_yearly_spend import SupplierYearlySpend
from app.models.transaction import Transaction

logger = logging.getLogger(__name__)


class CategorizationService:

    def __init__(self, db: Session):
        self.db = db

    def get_status(self, org_id: int) -> dict:
        """Get categorization progress for an organization."""
        total = self.db.query(Supplier).filter(Supplier.organization_id == org_id).count()
        # Use distinct supplier_id to avoid double-counting multi-category suppliers
        categorized = (
            self.db.query(func.count(func.distinct(SupplierCategorization.supplier_id)))
            .filter(SupplierCategorization.organization_id == org_id)
            .scalar() or 0
        )
        ai_suggested = (
            self.db.query(func.count(func.distinct(SupplierCategorization.supplier_id)))
            .filter(
                SupplierCategorization.organization_id == org_id,
                SupplierCategorization.source == "ai_suggested",
            )
            .scalar() or 0
        )
        ai_confirmed = (
            self.db.query(func.count(func.distinct(SupplierCategorization.supplier_id)))
            .filter(
                SupplierCategorization.organization_id == org_id,
                SupplierCategorization.source.in_(["ai_confirmed", "master_db"]),
            )
            .scalar() or 0
        )
        manually_set = (
            self.db.query(func.count(func.distinct(SupplierCategorization.supplier_id)))
            .filter(
                SupplierCategorization.organization_id == org_id,
                SupplierCategorization.source.in_(["manual", "ai_accepted"]),
            )
            .scalar() or 0
        )

        # Count uncategorized suppliers broken down by beinvloedbaar status
        categorized_exists = (
            self.db.query(SupplierCategorization.id)
            .filter(
                SupplierCategorization.supplier_id == Supplier.id,
                SupplierCategorization.organization_id == org_id,
            )
            .correlate(Supplier)
            .exists()
        )
        uncategorized_beinvloedbaar = (
            self.db.query(Supplier)
            .filter(
                Supplier.organization_id == org_id,
                Supplier.is_beinvloedbaar == True,  # noqa: E712
                ~categorized_exists,
            )
            .count()
        )
        uncategorized_niet_beinvloedbaar = (
            self.db.query(Supplier)
            .filter(
                Supplier.organization_id == org_id,
                Supplier.is_beinvloedbaar == False,  # noqa: E712
                ~categorized_exists,
            )
            .count()
        )

        return {
            "total_suppliers": total,
            "categorized_count": categorized,
            "uncategorized_count": total - categorized,
            "uncategorized_beinvloedbaar": uncategorized_beinvloedbaar,
            "uncategorized_niet_beinvloedbaar": uncategorized_niet_beinvloedbaar,
            "ai_suggested_count": ai_suggested,
            "ai_confirmed_count": ai_confirmed,
            "manually_set_count": manually_set,
            "percentage": round(categorized / total * 100, 1) if total > 0 else 0.0,
        }

    def get_supplier_descriptions(self, supplier_id: int, limit: int = 8) -> list[str]:
        """Get sample invoice descriptions for a supplier."""
        transactions = (
            self.db.query(Transaction.description)
            .filter(
                Transaction.supplier_id == supplier_id,
                Transaction.description.isnot(None),
                Transaction.description != "",
            )
            .distinct()
            .limit(limit)
            .all()
        )
        return [t.description for t in transactions if t.description]

    def get_all_categories(self, category_system: str = "aedes") -> list[dict]:
        """Get all categories for a specific system formatted for the AI prompt."""
        cats = (
            self.db.query(InkoopCategory)
            .filter(InkoopCategory.category_system == category_system)
            .order_by(InkoopCategory.nummer)
            .all()
        )
        return [
            {
                "id": c.id,
                "nummer": c.nummer,
                "groep": c.groep,
                "inkooppakket": c.inkooppakket,
                "definitie": c.definitie or "",
                "soort_inkoop": c.soort_inkoop or "",
            }
            for c in cats
        ]

    # Confidence threshold for auto-accepting AI suggestions
    AUTO_ACCEPT_THRESHOLD = 0.95
    # Max suppliers per single API call (to keep prompt size manageable)
    API_CALL_BATCH_SIZE = 10

    def get_uncategorized_counts(self, org_id: int) -> dict:
        """Get diagnostic counts for uncategorized suppliers."""
        from sqlalchemy import exists

        total = self.db.query(Supplier).filter(Supplier.organization_id == org_id).count()

        categorized = (
            self.db.query(func.count(func.distinct(SupplierCategorization.supplier_id)))
            .filter(SupplierCategorization.organization_id == org_id)
            .scalar() or 0
        )

        # NOT EXISTS is more reliable than NOT IN (avoids NULL-related issues)
        categorized_exists = (
            self.db.query(SupplierCategorization.id)
            .filter(
                SupplierCategorization.supplier_id == Supplier.id,
                SupplierCategorization.organization_id == org_id,
            )
            .correlate(Supplier)
            .exists()
        )

        uncategorized_beinvloedbaar = (
            self.db.query(Supplier)
            .filter(
                Supplier.organization_id == org_id,
                Supplier.is_beinvloedbaar == True,  # noqa: E712
                ~categorized_exists,
            )
            .count()
        )
        uncategorized_niet_beinvloedbaar = (
            self.db.query(Supplier)
            .filter(
                Supplier.organization_id == org_id,
                Supplier.is_beinvloedbaar == False,  # noqa: E712
                ~categorized_exists,
            )
            .count()
        )
        uncategorized_null_beinvloedbaar = (
            self.db.query(Supplier)
            .filter(
                Supplier.organization_id == org_id,
                Supplier.is_beinvloedbaar.is_(None),
                ~categorized_exists,
            )
            .count()
        )
        return {
            "total": total,
            "categorized": categorized,
            "uncategorized_beinvloedbaar": uncategorized_beinvloedbaar,
            "uncategorized_niet_beinvloedbaar": uncategorized_niet_beinvloedbaar,
            "uncategorized_null_beinvloedbaar": uncategorized_null_beinvloedbaar,
        }

    def ai_categorize_batch(
        self,
        org_id: int,
        supplier_ids: list[int] | None = None,
        batch_size: int = 10,
    ) -> tuple[list[dict], dict]:
        """
        Run AI categorization for a batch of uncategorized suppliers.

        Optimizations:
        1. Master DB pre-match: auto-categorize known suppliers (skip AI)
        2. Skip niet-beïnvloedbare suppliers (no AI needed)
        3. Prompt caching: reuse system prompt across sub-batches

        Returns tuple of (suggestions list, diagnostic counts dict).
        """
        import anthropic
        from app.api.settings import get_anthropic_api_key
        from app.services.supplier_master_service import SupplierMasterService

        # Get diagnostic counts first
        diagnostics = self.get_uncategorized_counts(org_id)
        logger.info(
            "AI categorize diagnostics for org %d: %s", org_id, diagnostics
        )

        api_key = get_anthropic_api_key(self.db)
        if not api_key:
            raise ValueError(
                "ANTHROPIC_API_KEY is niet geconfigureerd. Ga naar Instellingen om je API key in te voeren."
            )

        # Fix NULL is_beinvloedbaar values (set to True as intended default)
        if diagnostics["uncategorized_null_beinvloedbaar"] > 0:
            from sqlalchemy import text
            self.db.execute(
                text(
                    "UPDATE suppliers SET is_beinvloedbaar = TRUE "
                    "WHERE organization_id = :org_id AND is_beinvloedbaar IS NULL"
                ),
                {"org_id": org_id},
            )
            self.db.commit()
            logger.info(
                "Fixed %d suppliers with NULL is_beinvloedbaar",
                diagnostics["uncategorized_null_beinvloedbaar"],
            )
            # Refresh diagnostics after fix
            diagnostics = self.get_uncategorized_counts(org_id)

        # Use NOT EXISTS (more reliable than NOT IN for excluding categorized suppliers)
        from sqlalchemy import exists

        categorized_exists = (
            self.db.query(SupplierCategorization.id)
            .filter(
                SupplierCategorization.supplier_id == Supplier.id,
                SupplierCategorization.organization_id == org_id,
            )
            .correlate(Supplier)
            .exists()
        )

        query = (
            self.db.query(Supplier)
            .filter(
                Supplier.organization_id == org_id,
                ~categorized_exists,
            )
        )

        if supplier_ids:
            query = query.filter(Supplier.id.in_(supplier_ids))

        # Debug: log the count first
        debug_count = query.count()
        logger.info(
            "AI categorize query count for org %d: %d (before .all())",
            org_id, debug_count,
        )

        # Fetch uncategorized suppliers
        all_suppliers = query.all()

        # Sort by total spend in Python (avoids problematic outerjoin+group_by in SQL)
        if all_suppliers:
            spend_map = dict(
                self.db.query(
                    SupplierYearlySpend.supplier_id,
                    func.sum(SupplierYearlySpend.total_amount),
                )
                .filter(
                    SupplierYearlySpend.supplier_id.in_([s.id for s in all_suppliers])
                )
                .group_by(SupplierYearlySpend.supplier_id)
                .all()
            )
            all_suppliers.sort(
                key=lambda s: float(spend_map.get(s.id, 0) or 0), reverse=True
            )
            all_suppliers = all_suppliers[:batch_size]
        logger.info(
            "AI categorize found %d uncategorized suppliers for org %d",
            len(all_suppliers), org_id,
        )

        if not all_suppliers:
            return [], diagnostics

        # ── Step 1: Master DB pre-matching ──────────────────────────────
        # Auto-categorize suppliers that already exist in the cross-org
        # knowledge base, skipping AI entirely for these.
        category_system = "aedes"

        master_svc = SupplierMasterService(self.db)
        normalized_names = [
            s.normalized_name for s in all_suppliers if s.normalized_name
        ]
        master_lookups = master_svc.bulk_lookup(normalized_names, category_system=category_system)

        master_matched: list[dict] = []
        ai_needed: list = []

        for s in all_suppliers:
            matches = master_lookups.get(s.normalized_name, []) if s.normalized_name else []
            if matches:
                top_match = matches[0]
                # Direct categorization from master DB
                categorization = SupplierCategorization(
                    organization_id=org_id,
                    supplier_id=s.id,
                    category_id=top_match.category_id,
                    percentage=100.0,
                    source="master_db",
                    confidence=1.0,
                    ai_reasoning=(
                        f"Automatisch gekoppeld via Master Database "
                        f"(gebruikt bij {top_match.usage_count} organisatie(s))"
                    ),
                )
                self.db.add(categorization)

                # Increment usage count in master DB
                top_match.usage_count += 1

                master_matched.append({
                    "supplier_id": s.id,
                    "category_id": top_match.category_id,
                    "category_name": top_match.category_name,
                    "category_nummer": top_match.category_nummer,
                    "confidence": 1.0,
                    "reasoning": (
                        f"Master Database match "
                        f"({top_match.usage_count} organisatie(s))"
                    ),
                    "source": "master_db",
                })
            else:
                ai_needed.append(s)

        if master_matched:
            self.db.commit()
            logger.info(
                "Master DB pre-matched %d/%d suppliers, %d need AI",
                len(master_matched), len(all_suppliers), len(ai_needed),
            )

        if not ai_needed:
            return master_matched, diagnostics

        # ── Step 2: AI categorization for remaining suppliers ───────────
        categories = self.get_all_categories(category_system)
        category_text = "\n".join(
            [
                f"- Nr {c['nummer']}: {c['inkooppakket']} ({c['soort_inkoop']}) \u2014 {c['definitie'][:120]}"
                for c in categories
                if c["inkooppakket"]
            ]
        )

        client = anthropic.Anthropic(api_key=api_key)

        # Build system prompt once (will be cached by Anthropic across sub-batches)
        examples = self.get_confirmed_examples(org_id, limit=20)
        examples_text = ""
        if examples:
            examples_text = (
                "\n\nHier zijn voorbeelden van al bevestigde categoriseringen "
                "voor deze organisatie:\n"
            )
            for ex in examples:
                examples_text += (
                    f'- "{ex["supplier_name"]}" \u2192 '
                    f'{ex["category_nummer"]} {ex["category_name"]}\n'
                )
            examples_text += (
                "\nGebruik deze voorbeelden als context, maar baseer elke "
                "beslissing op de specifieke leverancier.\n"
            )

        cat_list_name = "PIANOo inkooppakkettenlijst"
        expertise = "Nederlandse publieke inkoop"

        system_prompt = (
            f"Je bent een expert in {expertise} en de {cat_list_name}.\n"
            f"Je taak is om leveranciers te classificeren in de juiste "
            f"inkooppakket-categorie.\n\n"
            f"Hieronder staat de volledige {cat_list_name}:\n\n"
            f"{category_text}\n\n"
            f"{examples_text}"
            "Regels:\n"
            "- Kies altijd precies \u00e9\u00e9n inkooppakket uit de lijst hierboven\n"
            "- Baseer je keuze op de leveranciersnaam EN de factuuromschrijvingen\n"
            "- Als de leverancier meerdere soorten producten/diensten levert, kies de "
            "categorie die het beste past bij het grootste deel van de uitgaven\n"
            "- Geef een confidence score van 0.0 tot 1.0 (1.0 = zeer zeker)\n"
            "- Leg kort uit waarom je deze categorie hebt gekozen (max 1 zin, in het "
            "Nederlands)\n\n"
            "Antwoord ALLEEN met een JSON array. Geen tekst ervoor of erna."
        )

        # Split into sub-batches
        ai_results = []
        for i in range(0, len(ai_needed), self.API_CALL_BATCH_SIZE):
            sub_batch = ai_needed[i : i + self.API_CALL_BATCH_SIZE]
            batch_num = i // self.API_CALL_BATCH_SIZE + 1
            total_batches = (len(ai_needed) + self.API_CALL_BATCH_SIZE - 1) // self.API_CALL_BATCH_SIZE
            logger.info(
                "Processing AI sub-batch %d/%d (%d suppliers)",
                batch_num, total_batches, len(sub_batch),
            )

            try:
                results = self._process_sub_batch(
                    org_id, sub_batch, system_prompt, client
                )
                ai_results.extend(results)
            except Exception as e:
                logger.error("Sub-batch %d failed: %s", batch_num, e)
                continue

        return master_matched + ai_results, diagnostics

    def _process_sub_batch(
        self,
        org_id: int,
        suppliers: list,
        system_prompt: str,
        client,
    ) -> list[dict]:
        """Process a single sub-batch of suppliers via Claude API.

        Uses prompt caching: the system prompt (categories + examples) is
        cached across sub-batches, reducing input tokens by ~90% after the
        first call.
        """
        # Build supplier info for this sub-batch
        supplier_entries = []
        for s in suppliers:
            descriptions = self.get_supplier_descriptions(s.id)
            total_spend = (
                self.db.query(func.sum(SupplierYearlySpend.total_amount))
                .filter(SupplierYearlySpend.supplier_id == s.id)
                .scalar()
                or 0
            )

            entry = f'Leverancier ID={s.id}: "{s.name}"\n  Totaal spend: \u20ac{total_spend:,.0f}'
            if descriptions:
                entry += "\n  Factuuromschrijvingen: " + "; ".join(descriptions[:5])
            supplier_entries.append(entry)

        suppliers_text = "\n\n".join(supplier_entries)

        user_prompt = (
            "Classificeer de volgende leverancier(s) in de juiste PIANOo categorie:\n\n"
            f"{suppliers_text}\n\n"
            "Antwoord met een JSON array:\n"
            '[{"supplier_id": <id>, "category_nummer": "<nummer>", '
            '"confidence": <0.0-1.0>, "reasoning": "<uitleg>"}]'
        )

        # Use prompt caching: system prompt is identical across sub-batches.
        # Anthropic caches it after the first call, subsequent calls pay ~10%
        # of input tokens for the cached portion (~8K tokens saved per call).
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            system=[
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_prompt}],
        )

        # Parse response
        content = response.content[0].text.strip()
        # Extract JSON from response (handle markdown code blocks)
        if "```" in content:
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
            content = content.strip()

        suggestions = json.loads(content)

        # Map category_nummer to category_id and save suggestions
        results = []
        for suggestion in suggestions:
            cat = (
                self.db.query(InkoopCategory)
                .filter(InkoopCategory.nummer == str(suggestion["category_nummer"]))
                .first()
            )

            if not cat:
                logger.warning(
                    "Category %s not found", suggestion["category_nummer"]
                )
                continue

            supplier_id = suggestion["supplier_id"]
            confidence = float(suggestion.get("confidence", 0.5))
            reasoning = suggestion.get("reasoning", "")

            # Auto-accept if confidence >= threshold
            source = (
                "ai_confirmed"
                if confidence >= self.AUTO_ACCEPT_THRESHOLD
                else "ai_suggested"
            )

            # Check if categorization already exists (scoped to org_id)
            existing = (
                self.db.query(SupplierCategorization)
                .filter(
                    SupplierCategorization.supplier_id == supplier_id,
                    SupplierCategorization.organization_id == org_id,
                )
                .all()
            )

            # Skip suppliers with manual multi-category assignments
            if len(existing) > 1:
                logger.info(
                    "Skipping supplier %d: has %d manual multi-category assignments",
                    supplier_id, len(existing),
                )
                continue

            if existing:
                existing[0].category_id = cat.id
                existing[0].source = source
                existing[0].confidence = confidence
                existing[0].ai_reasoning = reasoning
                existing[0].percentage = 100.0
            else:
                categorization = SupplierCategorization(
                    organization_id=org_id,
                    supplier_id=supplier_id,
                    category_id=cat.id,
                    percentage=100.0,
                    source=source,
                    confidence=confidence,
                    ai_reasoning=reasoning,
                )
                self.db.add(categorization)

            results.append(
                {
                    "supplier_id": supplier_id,
                    "category_id": cat.id,
                    "category_name": cat.inkooppakket,
                    "category_nummer": cat.nummer,
                    "confidence": confidence,
                    "reasoning": reasoning,
                    "source": source,
                }
            )

        self.db.commit()

        # Auto-populate master DB for confirmed results
        try:
            from app.services.supplier_master_service import SupplierMasterService
            master_svc = SupplierMasterService(self.db)
            for r in results:
                if r["source"] == "ai_confirmed":
                    supplier = self.db.query(Supplier).get(r["supplier_id"])
                    if supplier:
                        cat_obj = self.db.query(InkoopCategory).get(r["category_id"])
                        if cat_obj:
                            master_svc.upsert(
                                normalized_name=supplier.normalized_name,
                                display_name=supplier.name,
                                category_id=cat_obj.id,
                                category_nummer=cat_obj.nummer,
                                category_name=cat_obj.inkooppakket,
                                source="auto",
                            )
            self.db.commit()
        except Exception as e:
            logger.warning("Master DB auto-populate (AI batch) failed: %s", e)

        return results

    def set_category(
        self,
        org_id: int,
        supplier_id: int,
        category_id: int,
        source: str = "manual",
        user_id: int | None = None,
        confidence: float | None = None,
    ) -> SupplierCategorization:
        """Set or update a supplier's category (single-category: replaces all)."""
        # Verify supplier belongs to this organization
        supplier = (
            self.db.query(Supplier)
            .filter(Supplier.id == supplier_id, Supplier.organization_id == org_id)
            .first()
        )
        if not supplier:
            raise ValueError("Leverancier niet gevonden in deze organisatie")

        # Delete all existing categorizations for this supplier (scoped to org_id)
        self.db.query(SupplierCategorization).filter(
            SupplierCategorization.supplier_id == supplier_id,
            SupplierCategorization.organization_id == org_id,
        ).delete(synchronize_session="fetch")

        cat = SupplierCategorization(
            organization_id=org_id,
            supplier_id=supplier_id,
            category_id=category_id,
            percentage=100.0,
            source=source,
            confidence=confidence,
            categorized_by=user_id,
        )
        self.db.add(cat)
        self.db.commit()
        self.db.refresh(cat)

        # Auto-populate master database
        if source in ("manual", "ai_accepted"):
            try:
                from app.services.supplier_master_service import SupplierMasterService
                master_svc = SupplierMasterService(self.db)
                master_svc.record_categorization(supplier, cat)
                self.db.commit()
            except Exception as e:
                logger.warning("Master DB auto-populate failed: %s", e)

        return cat

    def get_suggestions(self, org_id: int) -> list:
        """Get all pending AI suggestions for review, ordered by spend DESC."""
        suggestions = (
            self.db.query(SupplierCategorization)
            .join(Supplier, Supplier.id == SupplierCategorization.supplier_id)
            .outerjoin(
                SupplierYearlySpend,
                SupplierYearlySpend.supplier_id == SupplierCategorization.supplier_id,
            )
            .filter(
                SupplierCategorization.organization_id == org_id,
                SupplierCategorization.source == "ai_suggested",
            )
            .group_by(SupplierCategorization.id)
            .order_by(func.sum(SupplierYearlySpend.total_amount).desc())
            .all()
        )
        return suggestions

    def bulk_accept(
        self, org_id: int, supplier_ids: list, user_id: int
    ) -> list:
        """Accept multiple AI suggestions: change source from ai_suggested to ai_accepted."""
        categorizations = (
            self.db.query(SupplierCategorization)
            .filter(
                SupplierCategorization.organization_id == org_id,
                SupplierCategorization.supplier_id.in_(supplier_ids),
                SupplierCategorization.source == "ai_suggested",
            )
            .all()
        )
        results = []
        for cat in categorizations:
            cat.source = "ai_accepted"
            cat.categorized_by = user_id
            results.append(
                {
                    "supplier_id": cat.supplier_id,
                    "category_id": cat.category_id,
                    "category_name": (
                        cat.category.inkooppakket if cat.category else None
                    ),
                }
            )
        self.db.commit()

        # Auto-populate master DB
        try:
            from app.services.supplier_master_service import SupplierMasterService
            master_svc = SupplierMasterService(self.db)
            for cat in categorizations:
                supplier = self.db.query(Supplier).get(cat.supplier_id)
                if supplier:
                    master_svc.record_categorization(supplier, cat)
            self.db.commit()
        except Exception as e:
            logger.warning("Master DB auto-populate (bulk_accept) failed: %s", e)

        return results

    def bulk_reject(self, org_id: int, supplier_ids: list) -> int:
        """Reject AI suggestions: delete the ai_suggested categorizations."""
        count = (
            self.db.query(SupplierCategorization)
            .filter(
                SupplierCategorization.organization_id == org_id,
                SupplierCategorization.supplier_id.in_(supplier_ids),
                SupplierCategorization.source == "ai_suggested",
            )
            .delete(synchronize_session="fetch")
        )
        self.db.commit()
        return count

    def get_confirmed_examples(self, org_id: int, limit: int = 20) -> list:
        """Get recently confirmed categorizations as examples for the AI prompt."""
        confirmed = (
            self.db.query(SupplierCategorization)
            .join(Supplier, Supplier.id == SupplierCategorization.supplier_id)
            .filter(
                SupplierCategorization.organization_id == org_id,
                SupplierCategorization.source.in_(
                    ["manual", "ai_accepted", "ai_confirmed"]
                ),
            )
            .order_by(SupplierCategorization.updated_at.desc())
            .limit(limit)
            .all()
        )
        examples = []
        for c in confirmed:
            supplier = self.db.query(Supplier).get(c.supplier_id)
            if supplier and c.category:
                examples.append(
                    {
                        "supplier_name": supplier.name,
                        "category_nummer": c.category.nummer,
                        "category_name": c.category.inkooppakket,
                    }
                )
        return examples
