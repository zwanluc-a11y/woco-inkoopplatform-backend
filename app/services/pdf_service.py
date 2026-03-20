"""PDF Report Service — generates professional procurement analysis reports.

Improvements:
- Table of contents
- Page numbers ("Pagina X van Y") in footer
- Header/footer on every page (logo, org name, Inkada branding)
- Charts (bar chart for spend, pie chart for risk & concentration)
- Spend trend over multiple years
- Top 10 suppliers section
- 80/20 concentration analysis
- Tables with repeatRows for multi-page overflow
"""
from __future__ import annotations

import io
import logging
from datetime import date
from pathlib import Path
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from reportlab.lib import colors as rl_colors
from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.pdfgen import canvas as canvas_module
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.piecharts import Pie
from reportlab.graphics.charts.barcharts import HorizontalBarChart

from app.models.contract import Contract, ContractSupplier
from app.models.organization import Organization
from app.models.category import InkoopCategory
from app.models.procurement_calendar_item import ProcurementCalendarItem
from app.models.risk_assessment import RiskAssessment
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_yearly_spend import SupplierYearlySpend
from app.models.threshold import Threshold

CATEGORY_SYSTEM_LABELS = {
    "aedes": "PIANOo",
}

ORG_TYPE_LABELS = {
    "woningcorporatie_klein": "Woningcorporatie klein (< 5.000 VHE)",
    "woningcorporatie_middel": "Woningcorporatie middel (5.000-20.000 VHE)",
    "woningcorporatie_groot": "Woningcorporatie groot (> 20.000 VHE)",
    "overig": "Overig",
}

logger = logging.getLogger(__name__)

# Default Inkada colors
DEFAULT_PRIMARY = "#1D2F45"
DEFAULT_SECONDARY = "#C8A45A"
DEFAULT_ACCENT = "#E8313A"

# Risk level colors
RISK_COLORS = {
    "offertetraject": HexColor("#ef4444"),
    "meervoudig_onderhands": HexColor("#eab308"),
    "enkelvoudig_onderhands": HexColor("#f97316"),
    "vrije_inkoop": HexColor("#10b981"),
}

RISK_LABELS = {
    "offertetraject": "Aanbesteden",
    "meervoudig_onderhands": "Onderzoek",
    "enkelvoudig_onderhands": "Monitoren",
    "vrije_inkoop": "Akkoord",
}

CHART_COLORS = [
    HexColor("#1D2F45"), HexColor("#C8A45A"), HexColor("#E8313A"),
    HexColor("#3B82F6"), HexColor("#10B981"), HexColor("#8B5CF6"),
    HexColor("#F59E0B"), HexColor("#EC4899"), HexColor("#6366F1"),
    HexColor("#14B8A6"),
]


class PDFReportService:
    """Generates a complete PDF report for an organization."""

    def __init__(self, db: Session):
        self.db = db

    @staticmethod
    def _resolve_logo_path(org) -> Optional[str]:
        """Get the logo file path, restoring from DB if the file is missing."""
        # Check if file exists on disk
        if org.brand_logo_path and Path(org.brand_logo_path).exists():
            return org.brand_logo_path

        # Restore from base64 data stored in DB (survives container restarts)
        if org.brand_logo_data:
            import base64
            logos_dir = Path(__file__).parent.parent.parent / "data" / "logos"
            logos_dir.mkdir(parents=True, exist_ok=True)
            restored_path = logos_dir / f"{org.id}_logo_restored.png"
            try:
                restored_path.write_bytes(base64.b64decode(org.brand_logo_data))
                return str(restored_path)
            except Exception:
                pass

        return None

    # ── Numbered Canvas with header / footer ────────────────────────────
    @staticmethod
    def _make_numbered_canvas(org_name: str, logo_path: Optional[str], primary_hex: str):
        """Return a Canvas subclass that draws headers, footers & page numbers."""

        class _NC(canvas_module.Canvas):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._saved_page_states: list[dict] = []

            def showPage(self):
                self._saved_page_states.append(dict(self.__dict__))
                self._startPage()

            def save(self):
                pages = list(self._saved_page_states)
                total = len(pages)
                for idx, state in enumerate(pages):
                    self.__dict__.update(state)
                    if idx > 0:  # skip cover page
                        _NC._draw_header_footer(self, idx + 1, total)
                    super(_NC, self).showPage()
                super(_NC, self).save()

            @staticmethod
            def _draw_header_footer(c, page_num: int, total_pages: int):
                w, h = A4
                # ── Header ──
                c.setStrokeColor(HexColor(primary_hex))
                c.setLineWidth(0.5)
                c.line(2 * cm, h - 1.8 * cm, w - 2 * cm, h - 1.8 * cm)
                # Small logo top-left
                if logo_path:
                    try:
                        c.drawImage(
                            logo_path, 2 * cm, h - 1.65 * cm,
                            width=0.8 * cm, height=0.8 * cm,
                            preserveAspectRatio=True, mask="auto",
                        )
                    except Exception:
                        pass
                # Org name top-right
                c.setFont("Helvetica", 8)
                c.setFillColor(HexColor("#666666"))
                c.drawRightString(w - 2 * cm, h - 1.55 * cm, org_name)
                # ── Footer ──
                c.setStrokeColor(HexColor(primary_hex))
                c.line(2 * cm, 1.5 * cm, w - 2 * cm, 1.5 * cm)
                c.setFont("Helvetica", 8)
                c.setFillColor(HexColor("#999999"))
                c.drawString(2 * cm, 1.0 * cm, "Inkada")
                c.setFillColor(HexColor("#666666"))
                c.drawRightString(
                    w - 2 * cm, 1.0 * cm,
                    f"Pagina {page_num} van {total_pages}",
                )

        return _NC

    # ── Styles ──────────────────────────────────────────────────────────
    @staticmethod
    def _create_styles(primary, secondary):
        styles = getSampleStyleSheet()
        styles.add(ParagraphStyle(
            name="CoverTitle", fontSize=28, textColor=primary,
            fontName="Helvetica-Bold", alignment=1, spaceAfter=12,
        ))
        styles.add(ParagraphStyle(
            name="CoverSubtitle", fontSize=14, textColor=HexColor("#666666"),
            fontName="Helvetica", alignment=1, spaceAfter=6,
        ))
        styles.add(ParagraphStyle(
            name="SectionTitle", fontSize=18, textColor=primary,
            fontName="Helvetica-Bold", spaceBefore=20, spaceAfter=12,
        ))
        styles.add(ParagraphStyle(
            name="SubTitle", fontSize=13, textColor=secondary,
            fontName="Helvetica-Bold", spaceBefore=12, spaceAfter=8,
        ))
        styles.add(ParagraphStyle(
            name="BodyText2", fontSize=10, textColor=HexColor("#333333"),
            fontName="Helvetica", spaceAfter=6, leading=14,
        ))
        styles.add(ParagraphStyle(
            name="BulletText", fontSize=10, textColor=HexColor("#333333"),
            fontName="Helvetica", leftIndent=20, spaceAfter=4, leading=14,
        ))
        return styles

    # ── Table helper ────────────────────────────────────────────────────
    @staticmethod
    def _base_table_style(primary, num_rows: int, font_size: int = 9, extra=None):
        """Standard table style: primary header, alternating rows."""
        cmds: list = [
            ("BACKGROUND", (0, 0), (-1, 0), primary),
            ("TEXTCOLOR", (0, 0), (-1, 0), rl_colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), font_size),
            ("GRID", (0, 0), (-1, -1), 0.5, HexColor("#dddddd")),
            ("PADDING", (0, 0), (-1, -1), 6),
        ]
        for i in range(1, num_rows):
            if i % 2 == 0:
                cmds.append(("BACKGROUND", (0, i), (-1, i), HexColor("#f7f7f7")))
        if extra:
            cmds.extend(extra)
        return TableStyle(cmds)

    # ── EUR formatter ───────────────────────────────────────────────────
    @staticmethod
    def _fmt_eur(amount: float) -> str:
        """Format as Dutch EUR."""
        if abs(amount) >= 1_000_000:
            return f"\u20ac {amount / 1_000_000:,.1f}M".replace(",", "X").replace(".", ",").replace("X", ".")
        if abs(amount) >= 1_000:
            return f"\u20ac {amount:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"\u20ac {amount:,.0f}".replace(",", ".")

    # ── Public API ──────────────────────────────────────────────────────
    def generate_report(self, org_id: int, assessment_year: int) -> io.BytesIO:
        """Generate a complete PDF report. Returns BytesIO buffer."""
        org = self.db.query(Organization).get(org_id)
        if not org:
            raise ValueError("Organisatie niet gevonden")

        primary = HexColor(org.brand_primary_color or DEFAULT_PRIMARY)
        secondary = HexColor(org.brand_secondary_color or DEFAULT_SECONDARY)
        accent = HexColor(org.brand_accent_color or DEFAULT_ACCENT)
        primary_hex = org.brand_primary_color or DEFAULT_PRIMARY
        logo_path = self._resolve_logo_path(org)

        data = self._gather_data(org_id, assessment_year)
        cat_label = CATEGORY_SYSTEM_LABELS.get(org.category_system, "PIANOo")

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer, pagesize=A4,
            leftMargin=2 * cm, rightMargin=2 * cm,
            topMargin=2.5 * cm, bottomMargin=2 * cm,
        )

        styles = self._create_styles(primary, secondary)
        story: list = []

        # ── Cover page ──
        self._build_cover(story, styles, org, assessment_year, logo_path)

        # ── Table of Contents ──
        self._build_toc(story, styles, primary, cat_label)
        story.append(PageBreak())

        # ── Section 1: Management Summary ──
        self._build_management_summary(story, styles, data, primary, secondary, cat_label)
        story.append(PageBreak())

        # ── Section 2: Niet-beïnvloedbare Spend ──
        self._build_niet_beinvloedbaar(story, styles, data, primary, secondary)
        story.append(PageBreak())

        # ── Section 3: Spend Analysis ──
        self._build_spend_analysis(story, styles, data, primary, secondary, cat_label)
        story.append(PageBreak())

        # ── Section 4: Spend Trend ──
        self._build_spend_trend(story, styles, data, primary, secondary)

        # ── Section 5: Top 10 Suppliers ──
        self._build_top_suppliers(story, styles, data, primary, secondary)
        story.append(PageBreak())

        # ── Section 6: Concentration 80/20 ──
        self._build_concentration(story, styles, data, primary, secondary)

        # ── Section 7: Leveranciers per Categoriegroep ──
        self._build_suppliers_per_groep(story, styles, data, primary, secondary, cat_label)
        story.append(PageBreak())

        # ── Section 8: Contractdekking ──
        self._build_contract_coverage(story, styles, data, primary, secondary)

        # ── Section 9: Leveranciersdynamiek ──
        self._build_supplier_dynamics(story, styles, data, primary, secondary)
        story.append(PageBreak())

        # ── Section 10: Categorie-ontwikkeling ──
        self._build_category_growth(story, styles, data, primary, secondary, cat_label)

        # ── Section 11: Risk Analysis ──
        self._build_risk_analysis(story, styles, data, primary, secondary)
        story.append(PageBreak())

        # ── Section 12: Recommendations ──
        self._build_recommendations(story, styles, data, primary, secondary, accent)

        # ── Section 13: Calendar ──
        self._build_calendar_planning(story, styles, data, primary, secondary)

        # Build PDF with numbered canvas
        nc_class = self._make_numbered_canvas(org.name, logo_path, primary_hex)
        doc.build(story, canvasmaker=nc_class)
        buffer.seek(0)
        return buffer

    # ── Cover ───────────────────────────────────────────────────────────
    @staticmethod
    def _build_cover(story, styles, org, assessment_year, logo_path):
        story.append(Spacer(1, 4 * cm))
        if logo_path:
            try:
                logo = Image(logo_path, width=6 * cm, height=6 * cm)
                logo.hAlign = "CENTER"
                story.append(logo)
                story.append(Spacer(1, 1 * cm))
            except Exception:
                pass
        story.append(Paragraph(org.name, styles["CoverTitle"]))
        story.append(Spacer(1, 0.5 * cm))
        story.append(Paragraph(
            "Inkoopanalyse &amp; WoCo Inkoopplatform", styles["CoverSubtitle"],
        ))
        story.append(Paragraph(str(assessment_year), styles["CoverSubtitle"]))
        story.append(Spacer(1, 2 * cm))
        story.append(Paragraph(
            f"Rapportdatum: {date.today().strftime('%d-%m-%Y')}",
            styles["CoverSubtitle"],
        ))
        story.append(Spacer(1, 1 * cm))
        story.append(Paragraph("Opgesteld door Inkada", styles["CoverSubtitle"]))
        story.append(PageBreak())

    # ── Table of Contents ───────────────────────────────────────────────
    @staticmethod
    def _build_toc(story, styles, primary, cat_label: str = "PIANOo"):
        story.append(Paragraph("Inhoudsopgave", styles["SectionTitle"]))
        story.append(Spacer(1, 1 * cm))

        sections = [
            ("1", "Management Samenvatting"),
            ("2", "Niet-be\u00efnvloedbare Spend"),
            ("3", "Spendanalyse"),
            ("4", "Spendtrend"),
            ("5", "Top 10 Leveranciers"),
            ("6", "Concentratie-analyse"),
            ("7", f"Leveranciers per {cat_label} Groep"),
            ("8", "Contractdekking"),
            ("9", "Leveranciersdynamiek"),
            ("10", "Categorie-ontwikkeling"),
            ("11", "Risicoanalyse"),
            ("12", "Aanbevelingen"),
            ("13", "WoCo Inkoopplatform"),
        ]
        rows = [[num, title] for num, title in sections]
        t = Table(rows, colWidths=[1.5 * cm, 14 * cm])
        t.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTNAME", (1, 0), (1, -1), "Helvetica"),
            ("FONTSIZE", (0, 0), (-1, -1), 12),
            ("TEXTCOLOR", (0, 0), (0, -1), primary),
            ("TEXTCOLOR", (1, 0), (1, -1), HexColor("#333333")),
            ("PADDING", (0, 0), (-1, -1), 8),
            ("LINEBELOW", (0, 0), (-1, -1), 0.5, HexColor("#eeeeee")),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(t)

    # ── Data Gathering ──────────────────────────────────────────────────
    def _gather_data(self, org_id: int, year: int) -> dict:
        """Gather all data needed for the report."""
        today = date.today()
        org = self.db.query(Organization).get(org_id)

        # --- Subquery: niet-beïnvloedbare supplier IDs ---
        niet_beinvloedbaar_sids = (
            self.db.query(Supplier.id)
            .filter(
                Supplier.organization_id == org_id,
                Supplier.is_beinvloedbaar == False,  # noqa: E712
            )
            .subquery()
        )

        # --- ALL spend (incl niet-beïnvloedbaar) for year reference ---
        yearly_spends_all = (
            self.db.query(
                SupplierYearlySpend.year,
                func.sum(SupplierYearlySpend.total_amount).label("total"),
            )
            .filter(SupplierYearlySpend.organization_id == org_id)
            .group_by(SupplierYearlySpend.year)
            .all()
        )
        spend_map_all = {r.year: float(r.total) for r in yearly_spends_all}
        most_recent_year = max(spend_map_all.keys()) if spend_map_all else year

        # Spend summary per year — only beïnvloedbaar
        yearly_spends = (
            self.db.query(
                SupplierYearlySpend.year,
                func.sum(SupplierYearlySpend.total_amount).label("total"),
                func.count(SupplierYearlySpend.supplier_id.distinct()).label("suppliers"),
            )
            .filter(
                SupplierYearlySpend.organization_id == org_id,
                ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
            )
            .group_by(SupplierYearlySpend.year)
            .order_by(SupplierYearlySpend.year)
            .all()
        )
        spend_map = {r.year: float(r.total) for r in yearly_spends}

        # Supplier count (all suppliers)
        supplier_count = (
            self.db.query(func.count(Supplier.id))
            .filter(Supplier.organization_id == org_id)
            .scalar() or 0
        )

        # Categorization (distinct supplier count for multi-category)
        categorized_count = (
            self.db.query(func.count(func.distinct(SupplierCategorization.supplier_id)))
            .filter(SupplierCategorization.organization_id == org_id)
            .scalar() or 0
        )

        # Spend by category (top 15, percentage-weighted) — only beïnvloedbaar
        cat_spends = (
            self.db.query(
                InkoopCategory.inkooppakket,
                InkoopCategory.groep,
                func.sum(
                    SupplierYearlySpend.total_amount
                    * SupplierCategorization.percentage / 100.0
                ).label("total_spend"),
                func.count(SupplierYearlySpend.supplier_id.distinct()).label("supplier_count"),
            )
            .join(SupplierCategorization, SupplierCategorization.category_id == InkoopCategory.id)
            .join(SupplierYearlySpend, SupplierYearlySpend.supplier_id == SupplierCategorization.supplier_id)
            .filter(
                SupplierCategorization.organization_id == org_id,
                SupplierYearlySpend.year == most_recent_year,
                ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
            )
            .group_by(InkoopCategory.inkooppakket, InkoopCategory.groep)
            .order_by(func.sum(SupplierYearlySpend.total_amount * SupplierCategorization.percentage / 100.0).desc())
            .limit(15)
            .all()
        )

        # Risk assessments
        risk_assessments = (
            self.db.query(RiskAssessment)
            .filter(RiskAssessment.organization_id == org_id)
            .order_by(RiskAssessment.estimated_contract_value.desc())
            .all()
        )
        risk_summary = {"offertetraject": 0, "meervoudig_onderhands": 0, "enkelvoudig_onderhands": 0, "vrije_inkoop": 0}
        for r in risk_assessments:
            if r.risk_level in risk_summary:
                risk_summary[r.risk_level] += 1

        # Pre-load categories for risk assessments (avoid N+1)
        risk_cat_ids = {r.category_id for r in risk_assessments}
        risk_categories: dict = {}
        if risk_cat_ids:
            for c in self.db.query(InkoopCategory).filter(InkoopCategory.id.in_(risk_cat_ids)).all():
                risk_categories[c.id] = c

        # Expiring contracts (within 2 years)
        future_limit = date(today.year + 2, today.month, today.day)
        expiring = (
            self.db.query(Contract)
            .filter(
                Contract.organization_id == org_id,
                Contract.end_date != None,  # noqa: E711
                Contract.end_date >= today,
                Contract.end_date <= future_limit,
            )
            .order_by(Contract.end_date.asc())
            .all()
        )

        # Calendar items
        calendar_items = (
            self.db.query(ProcurementCalendarItem)
            .filter(ProcurementCalendarItem.organization_id == org_id)
            .order_by(
                ProcurementCalendarItem.priority.desc(),
                ProcurementCalendarItem.target_start_date,
            )
            .all()
        )

        # ── Top 10 suppliers by spend — only beïnvloedbaar ──
        top_suppliers = (
            self.db.query(
                Supplier.name,
                func.sum(SupplierYearlySpend.total_amount).label("total_spend"),
            )
            .join(SupplierYearlySpend, SupplierYearlySpend.supplier_id == Supplier.id)
            .filter(
                Supplier.organization_id == org_id,
                SupplierYearlySpend.organization_id == org_id,
                SupplierYearlySpend.year == most_recent_year,
                ~Supplier.id.in_(niet_beinvloedbaar_sids),
            )
            .group_by(Supplier.id, Supplier.name)
            .order_by(func.sum(SupplierYearlySpend.total_amount).desc())
            .limit(10)
            .all()
        )

        # ── 80/20 concentration — only beïnvloedbaar ──
        all_supplier_spends = (
            self.db.query(
                func.sum(SupplierYearlySpend.total_amount).label("total_spend"),
            )
            .filter(
                SupplierYearlySpend.organization_id == org_id,
                SupplierYearlySpend.year == most_recent_year,
                ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
            )
            .group_by(SupplierYearlySpend.supplier_id)
            .order_by(func.sum(SupplierYearlySpend.total_amount).desc())
            .all()
        )
        total_supplier_spend = sum(float(s.total_spend) for s in all_supplier_spends)
        running = 0.0
        count_80 = 0
        for s in all_supplier_spends:
            running += float(s.total_spend)
            count_80 += 1
            if total_supplier_spend > 0 and running >= total_supplier_spend * 0.8:
                break

        concentration = {
            "total_suppliers": len(all_supplier_spends),
            "suppliers_80pct": count_80,
            "spend_80pct": running,
            "total_spend": total_supplier_spend,
        }

        # ── Niet-beïnvloedbaar section data ──
        nb_supplier_count = (
            self.db.query(func.count(Supplier.id))
            .filter(
                Supplier.organization_id == org_id,
                Supplier.is_beinvloedbaar == False,  # noqa: E712
            )
            .scalar() or 0
        )
        nb_spend_current = spend_map_all.get(most_recent_year, 0) - spend_map.get(most_recent_year, 0)

        # Top niet-beïnvloedbaar suppliers
        nb_top_suppliers = (
            self.db.query(
                Supplier.name,
                SupplierYearlySpend.total_amount,
            )
            .join(SupplierYearlySpend, SupplierYearlySpend.supplier_id == Supplier.id)
            .filter(
                SupplierYearlySpend.organization_id == org_id,
                SupplierYearlySpend.year == most_recent_year,
                Supplier.is_beinvloedbaar == False,  # noqa: E712
            )
            .order_by(SupplierYearlySpend.total_amount.desc())
            .limit(10)
            .all()
        )

        niet_beinvloedbaar = {
            "total_spend": nb_spend_current,
            "supplier_count": nb_supplier_count,
            "top_suppliers": [
                {"name": s.name, "total_spend": float(s.total_amount)}
                for s in nb_top_suppliers
            ],
        }

        total_spend = spend_map.get(most_recent_year, 0)
        total_all_cats = sum(float(c.total_spend) for c in cat_spends) if cat_spends else 0

        # ── Spend per groep (supplier count per group) ──
        spend_by_groep = (
            self.db.query(
                InkoopCategory.groep,
                func.sum(
                    SupplierYearlySpend.total_amount
                    * SupplierCategorization.percentage / 100.0
                ).label("total_spend"),
                func.count(SupplierYearlySpend.supplier_id.distinct()).label("supplier_count"),
            )
            .join(SupplierCategorization, SupplierCategorization.category_id == InkoopCategory.id)
            .join(SupplierYearlySpend, SupplierYearlySpend.supplier_id == SupplierCategorization.supplier_id)
            .filter(
                SupplierCategorization.organization_id == org_id,
                SupplierYearlySpend.year == most_recent_year,
                ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
            )
            .group_by(InkoopCategory.groep)
            .order_by(func.count(SupplierYearlySpend.supplier_id.distinct()).desc())
            .all()
        )

        # ── Contract coverage ──
        contracted_supplier_ids = set()
        active_contracts = (
            self.db.query(Contract)
            .filter(
                Contract.organization_id == org_id,
                Contract.status.in_(["active", "expiring"]),
            )
            .all()
        )
        for contract in active_contracts:
            supplier_ids = (
                self.db.query(ContractSupplier.supplier_id)
                .filter(ContractSupplier.contract_id == contract.id)
                .all()
            )
            for (sid,) in supplier_ids:
                contracted_supplier_ids.add(sid)

        # Calculate covered vs uncovered spend
        covered_spend = 0.0
        if contracted_supplier_ids:
            covered_result = (
                self.db.query(func.sum(SupplierYearlySpend.total_amount))
                .filter(
                    SupplierYearlySpend.organization_id == org_id,
                    SupplierYearlySpend.year == most_recent_year,
                    SupplierYearlySpend.supplier_id.in_(contracted_supplier_ids),
                    ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
                )
                .scalar()
            )
            covered_spend = float(covered_result or 0)

        maverick_spend = max(total_spend - covered_spend, 0)
        coverage_pct = round(covered_spend / total_spend * 100, 1) if total_spend > 0 else 0

        contract_coverage = {
            "covered_spend": covered_spend,
            "maverick_spend": maverick_spend,
            "coverage_pct": coverage_pct,
            "active_contracts": len(active_contracts),
        }

        # ── Supplier dynamics (new/lost) ──
        supplier_dynamics = None
        sorted_years = sorted(spend_map.keys())
        if len(sorted_years) >= 2:
            current_yr = sorted_years[-1]
            prev_yr = sorted_years[-2]
            current_sids = set(
                r[0] for r in self.db.query(SupplierYearlySpend.supplier_id)
                .filter(
                    SupplierYearlySpend.organization_id == org_id,
                    SupplierYearlySpend.year == current_yr,
                    ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
                ).all()
            )
            prev_sids = set(
                r[0] for r in self.db.query(SupplierYearlySpend.supplier_id)
                .filter(
                    SupplierYearlySpend.organization_id == org_id,
                    SupplierYearlySpend.year == prev_yr,
                    ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
                ).all()
            )
            new_sids = current_sids - prev_sids
            lost_sids = prev_sids - current_sids
            new_spend = 0.0
            if new_sids:
                ns = self.db.query(func.sum(SupplierYearlySpend.total_amount)).filter(
                    SupplierYearlySpend.organization_id == org_id,
                    SupplierYearlySpend.year == current_yr,
                    SupplierYearlySpend.supplier_id.in_(new_sids),
                ).scalar()
                new_spend = float(ns or 0)
            lost_spend = 0.0
            if lost_sids:
                ls = self.db.query(func.sum(SupplierYearlySpend.total_amount)).filter(
                    SupplierYearlySpend.organization_id == org_id,
                    SupplierYearlySpend.year == prev_yr,
                    SupplierYearlySpend.supplier_id.in_(lost_sids),
                ).scalar()
                lost_spend = float(ls or 0)
            supplier_dynamics = {
                "new_count": len(new_sids),
                "new_spend": new_spend,
                "lost_count": len(lost_sids),
                "lost_spend": lost_spend,
                "compare_year": prev_yr,
            }

        # ── Category growth (top growers/decliners) ──
        category_growth = {"top_growers": [], "top_decliners": []}
        if len(sorted_years) >= 2:
            current_yr = sorted_years[-1]
            prev_yr = sorted_years[-2]
            for yr in [current_yr, prev_yr]:
                pass  # just need the years
            cat_spend_by_year = {}
            for yr in [current_yr, prev_yr]:
                rows = (
                    self.db.query(
                        InkoopCategory.groep,
                        func.sum(
                            SupplierYearlySpend.total_amount
                            * SupplierCategorization.percentage / 100.0
                        ).label("total"),
                    )
                    .join(SupplierCategorization, SupplierCategorization.category_id == InkoopCategory.id)
                    .join(SupplierYearlySpend, SupplierYearlySpend.supplier_id == SupplierCategorization.supplier_id)
                    .filter(
                        SupplierCategorization.organization_id == org_id,
                        SupplierYearlySpend.year == yr,
                        ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
                    )
                    .group_by(InkoopCategory.groep)
                    .all()
                )
                cat_spend_by_year[yr] = {r.groep: float(r.total) for r in rows}

            cur_data = cat_spend_by_year.get(current_yr, {})
            prev_data = cat_spend_by_year.get(prev_yr, {})
            all_groepen = set(cur_data.keys()) | set(prev_data.keys())
            changes = []
            for groep in all_groepen:
                cur = cur_data.get(groep, 0)
                prev = prev_data.get(groep, 0)
                if prev > 10000:  # only meaningful categories
                    pct = round((cur - prev) / prev * 100, 1)
                    changes.append({
                        "groep": groep,
                        "current_spend": cur,
                        "previous_spend": prev,
                        "growth_pct": pct,
                        "absolute_change": cur - prev,
                    })
            changes.sort(key=lambda x: x["growth_pct"], reverse=True)
            category_growth["top_growers"] = [c for c in changes if c["growth_pct"] > 0][:5]
            category_growth["top_decliners"] = [c for c in changes if c["growth_pct"] < 0][:5]

        return {
            "year": most_recent_year,
            "org": org,
            "total_spend": total_spend,
            "spend_map": spend_map,
            "yearly_spends": yearly_spends,
            "supplier_count": supplier_count,
            "categorized_count": categorized_count,
            "cat_spends": cat_spends,
            "total_all_cats": total_all_cats,
            "risk_assessments": risk_assessments,
            "risk_summary": risk_summary,
            "risk_categories": risk_categories,
            "expiring_contracts": expiring,
            "calendar_items": calendar_items,
            "top_suppliers": top_suppliers,
            "concentration": concentration,
            "niet_beinvloedbaar": niet_beinvloedbaar,
            "total_spend_all": spend_map_all.get(most_recent_year, 0),
            "spend_by_groep": spend_by_groep,
            "contract_coverage": contract_coverage,
            "supplier_dynamics": supplier_dynamics,
            "category_growth": category_growth,
        }

    # ── Section 1: Management Summary ───────────────────────────────────
    def _build_management_summary(self, story, styles, data, primary, secondary, cat_label: str = "PIANOo"):
        story.append(Paragraph("1. Management Samenvatting", styles["SectionTitle"]))

        nb = data["niet_beinvloedbaar"]
        total_incl_nb = data["total_spend_all"]
        org = data.get("org")
        org_type_label = ORG_TYPE_LABELS.get(org.org_type, "Organisatie") if org else "Organisatie"

        total_risk = data["risk_summary"]["offertetraject"] + data["risk_summary"]["meervoudig_onderhands"]
        cat_pct = (
            round(data["categorized_count"] / data["supplier_count"] * 100, 1)
            if data["supplier_count"] > 0 else 0
        )
        conc = data["concentration"]
        cov = data.get("contract_coverage", {})

        findings = [
            f"De totale be\u00efnvloedbare inkoopomvang in {data['year']} bedraagt "
            f"<b>{self._fmt_eur(data['total_spend'])}</b> verdeeld over "
            f"<b>{data['supplier_count'] - nb['supplier_count']}</b> leveranciers "
            f"(exclusief {self._fmt_eur(nb['total_spend'])} niet-be\u00efnvloedbare spend).",

            f"Van alle leveranciers is <b>{cat_pct}%</b> gecategoriseerd naar "
            f"{cat_label}-inkooppakketten "
            f"({data['categorized_count']} van {data['supplier_count']}).",

            f"Er zijn <b>{total_risk}</b> inkooppakketten ge\u00efdentificeerd die (mogelijk) boven de "
            f"aanbestedingsdrempel uitkomen: {data['risk_summary']['offertetraject']} aanbesteden, "
            f"{data['risk_summary']['meervoudig_onderhands']} onderzoek.",

            f"De contractdekking bedraagt <b>{cov.get('coverage_pct', 0)}%</b> "
            f"({self._fmt_eur(cov.get('covered_spend', 0))} gedekt, "
            f"{self._fmt_eur(cov.get('maverick_spend', 0))} zonder contract).",

            f"Er lopen <b>{len(data['expiring_contracts'])}</b> contracten af binnen 2 jaar.",

            f"De inkoopkalender bevat <b>{len(data['calendar_items'])}</b> geplande "
            f"aanbestedingstrajecten.",
        ]

        # Add concentration finding
        if conc["total_suppliers"] > 0:
            pct_supp = round(conc["suppliers_80pct"] / conc["total_suppliers"] * 100, 1)
            findings.append(
                f"<b>{conc['suppliers_80pct']}</b> leveranciers ({pct_supp}%) zijn verantwoordelijk "
                f"voor 80% van de totale uitgaven (concentratie-analyse)."
            )

        # Add supplier dynamics finding
        dyn = data.get("supplier_dynamics")
        if dyn:
            findings.append(
                f"In {data['year']} zijn <b>{dyn['new_count']}</b> nieuwe leveranciers toegevoegd "
                f"({self._fmt_eur(dyn['new_spend'])}) en <b>{dyn['lost_count']}</b> leveranciers "
                f"uitgestroomd ({self._fmt_eur(dyn['lost_spend'])})."
            )

        for f in findings:
            story.append(Paragraph(f"\u2022 {f}", styles["BulletText"]))
        story.append(Spacer(1, 0.5 * cm))

        # Summary table
        summary_data = [
            ["Kerngegevens", str(data["year"])],
            ["Totale spend", self._fmt_eur(data["total_spend"])],
            ["Aantal leveranciers", str(data["supplier_count"])],
            ["Categorisatie", f"{cat_pct}%"],
            ["Boven drempel", str(total_risk)],
            ["Aflopende contracten", str(len(data["expiring_contracts"]))],
            ["Kalenderitems", str(len(data["calendar_items"]))],
        ]
        t = Table(summary_data, colWidths=[8 * cm, 6 * cm], repeatRows=1)
        t.setStyle(self._base_table_style(primary, len(summary_data), extra=[
            ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ]))
        story.append(t)

    # ── Section 2: Niet-beïnvloedbare Spend ───────────────────────────
    def _build_niet_beinvloedbaar(self, story, styles, data, primary, secondary):
        story.append(Paragraph(
            "2. Niet-be\u00efnvloedbare Spend", styles["SectionTitle"],
        ))

        nb = data["niet_beinvloedbaar"]
        total_all = data["total_spend_all"]

        if nb["supplier_count"] == 0 or nb["total_spend"] <= 0:
            story.append(Paragraph(
                "Er zijn geen leveranciers gemarkeerd als niet-be\u00efnvloedbaar. "
                "Alle spend is meegenomen in de analyse.",
                styles["BodyText2"],
            ))
            return

        pct_of_total = (
            round(nb["total_spend"] / total_all * 100, 1)
            if total_all > 0 else 0
        )

        story.append(Paragraph(
            f"Niet-be\u00efnvloedbare leveranciers zijn partijen waarbij de organisatie "
            f"geen of beperkte keuze heeft in de inkoop, zoals belastingen, "
            f"nutsbedrijven, overheidsheffingen en wettelijk verplichte afdrachten. "
            f"Deze spend is <b>uitgesloten</b> uit de overige analyses in dit rapport.",
            styles["BodyText2"],
        ))
        story.append(Spacer(1, 0.3 * cm))

        # Summary bullets
        story.append(Paragraph(
            f"\u2022 Totale niet-be\u00efnvloedbare spend: "
            f"<b>{self._fmt_eur(nb['total_spend'])}</b> "
            f"({pct_of_total}% van totale uitgaven {self._fmt_eur(total_all)})",
            styles["BulletText"],
        ))
        story.append(Paragraph(
            f"\u2022 Aantal niet-be\u00efnvloedbare leveranciers: "
            f"<b>{nb['supplier_count']}</b>",
            styles["BulletText"],
        ))
        story.append(Spacer(1, 0.5 * cm))

        # Top niet-beïnvloedbare suppliers table
        if nb["top_suppliers"]:
            story.append(Paragraph(
                "Grootste niet-be\u00efnvloedbare leveranciers",
                styles["SubTitle"],
            ))
            header = ["#", "Leverancier", "Spend", "% Totaal"]
            rows = [header]
            for i, s in enumerate(nb["top_suppliers"]):
                spend = s["total_spend"]
                pct = round(spend / total_all * 100, 1) if total_all > 0 else 0
                rows.append([
                    str(i + 1),
                    (s["name"] or "Onbekend")[:45],
                    self._fmt_eur(spend),
                    f"{pct}%",
                ])

            t = Table(
                rows,
                colWidths=[1 * cm, 9 * cm, 3.5 * cm, 2.5 * cm],
                repeatRows=1,
            )
            t.setStyle(self._base_table_style(primary, len(rows), extra=[
                ("ALIGN", (0, 0), (0, -1), "CENTER"),
                ("ALIGN", (2, 0), (2, -1), "RIGHT"),
                ("ALIGN", (3, 0), (3, -1), "RIGHT"),
            ]))
            story.append(t)

    # ── Section 3: Spend Analysis ───────────────────────────────────────
    def _build_spend_analysis(self, story, styles, data, primary, secondary, cat_label: str = "PIANOo"):
        story.append(Paragraph("3. Spendanalyse", styles["SectionTitle"]))
        story.append(Paragraph(
            f"Onderstaande grafiek en tabel tonen de top 15 inkooppakketten "
            f"op basis van be\u00efnvloedbare uitgaven in {data['year']} "
            f"(exclusief niet-be\u00efnvloedbare spend).",
            styles["BodyText2"],
        ))
        story.append(Spacer(1, 0.3 * cm))

        # ── Bar chart (top 10) ──
        chart = self._build_spend_chart(data["cat_spends"], primary)
        if chart:
            story.append(Paragraph("Verdeling per inkooppakket", styles["SubTitle"]))
            story.append(chart)
            story.append(Spacer(1, 0.5 * cm))

        # ── Category spend table ──
        story.append(Paragraph("Detail per inkooppakket", styles["SubTitle"]))
        header = ["#", "Inkooppakket", "Groep", "Spend", "% Totaal", "Lev."]
        rows = [header]
        for i, c in enumerate(data["cat_spends"]):
            spend = float(c.total_spend)
            pct = round(spend / data["total_all_cats"] * 100, 1) if data["total_all_cats"] > 0 else 0
            rows.append([
                str(i + 1),
                c.inkooppakket[:40],
                c.groep[:20],
                self._fmt_eur(spend),
                f"{pct}%",
                str(c.supplier_count),
            ])

        t = Table(rows, colWidths=[1 * cm, 7 * cm, 3.5 * cm, 2.5 * cm, 1.5 * cm, 1.2 * cm], repeatRows=1)
        t.setStyle(self._base_table_style(primary, len(rows), font_size=8, extra=[
            ("ALIGN", (0, 0), (0, -1), "CENTER"),
            ("ALIGN", (3, 0), (3, -1), "RIGHT"),
            ("ALIGN", (4, 0), (4, -1), "RIGHT"),
            ("ALIGN", (5, 0), (5, -1), "CENTER"),
        ]))
        story.append(t)

    def _build_spend_chart(self, cat_spends, primary):
        """Create a horizontal bar chart for top spend categories."""
        items = list(cat_spends[:10])
        if not items:
            return None

        # Reverse so largest appears at top
        items = list(reversed(items))

        chart_height = max(180, len(items) * 24)
        d = Drawing(480, chart_height + 40)
        bc = HorizontalBarChart()
        bc.x = 170
        bc.y = 20
        bc.width = 280
        bc.height = chart_height
        bc.data = [[float(c.total_spend) for c in items]]
        bc.categoryAxis.categoryNames = [
            (c.inkooppakket[:30] + "\u2026" if len(c.inkooppakket) > 30 else c.inkooppakket)
            for c in items
        ]
        bc.categoryAxis.labels.fontName = "Helvetica"
        bc.categoryAxis.labels.fontSize = 7
        bc.categoryAxis.labels.textAnchor = "end"
        bc.categoryAxis.labels.dx = -8
        bc.valueAxis.valueMin = 0
        bc.valueAxis.labels.fontName = "Helvetica"
        bc.valueAxis.labels.fontSize = 7
        bc.valueAxis.labelTextFormat = lambda v: self._fmt_eur(v) if v > 0 else ""
        bc.bars[0].fillColor = primary
        bc.bars[0].strokeColor = None
        d.add(bc)
        return d

    # ── Section 4: Spend Trend ──────────────────────────────────────────
    def _build_spend_trend(self, story, styles, data, primary, secondary):
        story.append(Paragraph("4. Spendtrend", styles["SectionTitle"]))

        spend_map = data["spend_map"]
        if len(spend_map) <= 1:
            story.append(Paragraph(
                "Er is slechts data beschikbaar voor \u00e9\u00e9n jaar, "
                "waardoor geen trend getoond kan worden.",
                styles["BodyText2"],
            ))
            story.append(Spacer(1, 0.5 * cm))
            return

        story.append(Paragraph(
            f"Onderstaand overzicht toont de ontwikkeling van de totale "
            f"inkoopuitgaven over {len(spend_map)} jaar.",
            styles["BodyText2"],
        ))
        story.append(Spacer(1, 0.3 * cm))

        years = sorted(spend_map.keys())
        header = ["Jaar", "Totale Spend", "Verschil t.o.v. vorig jaar"]
        rows = [header]
        prev = None
        for y in years:
            amount = spend_map[y]
            if prev is not None and prev > 0:
                diff = amount - prev
                diff_pct = round(diff / prev * 100, 1)
                sign = "+" if diff >= 0 else ""
                diff_str = f"{sign}{self._fmt_eur(diff)} ({sign}{diff_pct}%)"
            else:
                diff_str = "-"
            rows.append([str(y), self._fmt_eur(amount), diff_str])
            prev = amount

        t = Table(rows, colWidths=[3 * cm, 5 * cm, 7 * cm], repeatRows=1)
        t.setStyle(self._base_table_style(primary, len(rows), extra=[
            ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ]))
        story.append(t)
        story.append(Spacer(1, 1 * cm))

    # ── Section 5: Top 10 Suppliers ─────────────────────────────────────
    def _build_top_suppliers(self, story, styles, data, primary, secondary):
        story.append(Paragraph("5. Top 10 Leveranciers", styles["SectionTitle"]))

        top = data["top_suppliers"]
        if not top:
            story.append(Paragraph(
                "Er zijn geen leveranciers met spend-data beschikbaar.",
                styles["BodyText2"],
            ))
            story.append(Spacer(1, 0.5 * cm))
            return

        total = data["total_spend"] or 1
        story.append(Paragraph(
            f"De tien grootste leveranciers op basis van uitgaven in {data['year']}.",
            styles["BodyText2"],
        ))
        story.append(Spacer(1, 0.3 * cm))

        header = ["#", "Leverancier", "Spend", "% Totaal"]
        rows = [header]
        for i, s in enumerate(top):
            spend = float(s.total_spend)
            pct = round(spend / total * 100, 1) if total > 0 else 0
            rows.append([
                str(i + 1),
                (s.name or "Onbekend")[:45],
                self._fmt_eur(spend),
                f"{pct}%",
            ])

        t = Table(rows, colWidths=[1 * cm, 9 * cm, 3.5 * cm, 2.5 * cm], repeatRows=1)
        t.setStyle(self._base_table_style(primary, len(rows), extra=[
            ("ALIGN", (0, 0), (0, -1), "CENTER"),
            ("ALIGN", (2, 0), (2, -1), "RIGHT"),
            ("ALIGN", (3, 0), (3, -1), "RIGHT"),
        ]))
        story.append(t)
        story.append(Spacer(1, 0.5 * cm))

    # ── Section 6: Concentration 80/20 ──────────────────────────────────
    def _build_concentration(self, story, styles, data, primary, secondary):
        story.append(Paragraph("6. Concentratie-analyse", styles["SectionTitle"]))

        conc = data["concentration"]
        total_suppliers = conc["total_suppliers"]
        suppliers_80 = conc["suppliers_80pct"]

        if total_suppliers == 0:
            story.append(Paragraph(
                "Er zijn onvoldoende gegevens om een concentratie-analyse uit te voeren.",
                styles["BodyText2"],
            ))
            story.append(Spacer(1, 0.5 * cm))
            return

        pct_suppliers = round(suppliers_80 / total_suppliers * 100, 1)
        story.append(Paragraph(
            f"Van de <b>{total_suppliers}</b> leveranciers zijn slechts "
            f"<b>{suppliers_80}</b> leveranciers ({pct_suppliers}%) verantwoordelijk "
            f"voor 80% van de totale uitgaven "
            f"({self._fmt_eur(conc['spend_80pct'])} van "
            f"{self._fmt_eur(conc['total_spend'])}).",
            styles["BodyText2"],
        ))
        story.append(Paragraph(
            "Dit duidt op een hoge leveranciersconcentratie. "
            "Het is aan te bevelen om de afhankelijkheid van deze "
            "topleveranciers te monitoren.",
            styles["BodyText2"],
        ))
        story.append(Spacer(1, 0.3 * cm))

        # Pie chart: 80% vs 20%
        rest_spend = conc["total_spend"] - conc["spend_80pct"]
        rest_count = total_suppliers - suppliers_80

        d = Drawing(420, 180)
        pie = Pie()
        pie.x = 100
        pie.y = 10
        pie.width = 140
        pie.height = 140
        pie.data = [conc["spend_80pct"], max(rest_spend, 0.01)]
        pie.labels = [
            f"Top {suppliers_80} leveranciers (80%)",
            f"Overige {rest_count} leveranciers (20%)",
        ]
        pie.sideLabels = True
        pie.simpleLabels = False
        pie.slices.fontName = "Helvetica"
        pie.slices.fontSize = 8
        pie.slices[0].fillColor = primary
        pie.slices[1].fillColor = HexColor("#D1D5DB")
        d.add(pie)
        story.append(d)
        story.append(Spacer(1, 1 * cm))

    # ── Section 7: Suppliers per Category Group ────────────────────────
    def _build_suppliers_per_groep(self, story, styles, data, primary, secondary, cat_label: str = "PIANOo"):
        story.append(Paragraph(f"7. Leveranciers per {cat_label} Groep", styles["SectionTitle"]))

        groep_data = data.get("spend_by_groep", [])
        if not groep_data:
            story.append(Paragraph(
                "Er zijn geen gecategoriseerde leveranciers beschikbaar.",
                styles["BodyText2"],
            ))
            story.append(Spacer(1, 0.5 * cm))
            return

        story.append(Paragraph(
            f"Onderstaand overzicht toont het aantal leveranciers per {cat_label}-categoriegroep, "
            f"gesorteerd op aantal leveranciers (aflopend). Dit helpt bij het identificeren van "
            f"bundelkansen in categorie\u00ebn met veel leveranciers.",
            styles["BodyText2"],
        ))
        story.append(Spacer(1, 0.3 * cm))

        header = ["#", f"{cat_label} Groep", "Leveranciers", "Spend", "Gem. per lev."]
        rows = [header]
        for i, g in enumerate(groep_data[:15]):
            spend = float(g.total_spend)
            avg = spend / g.supplier_count if g.supplier_count > 0 else 0
            rows.append([
                str(i + 1),
                (g.groep or "Onbekend")[:35],
                str(g.supplier_count),
                self._fmt_eur(spend),
                self._fmt_eur(avg),
            ])

        t = Table(rows, colWidths=[1 * cm, 7 * cm, 2.5 * cm, 3 * cm, 3 * cm], repeatRows=1)
        t.setStyle(self._base_table_style(primary, len(rows), extra=[
            ("ALIGN", (0, 0), (0, -1), "CENTER"),
            ("ALIGN", (2, 0), (2, -1), "CENTER"),
            ("ALIGN", (3, 0), (3, -1), "RIGHT"),
            ("ALIGN", (4, 0), (4, -1), "RIGHT"),
        ]))
        story.append(t)
        story.append(Spacer(1, 0.5 * cm))

    # ── Section 8: Contract Coverage ─────────────────────────────────
    def _build_contract_coverage(self, story, styles, data, primary, secondary):
        story.append(Paragraph("8. Contractdekking", styles["SectionTitle"]))

        cov = data.get("contract_coverage", {})
        coverage_pct = cov.get("coverage_pct", 0)
        covered = cov.get("covered_spend", 0)
        maverick = cov.get("maverick_spend", 0)
        active = cov.get("active_contracts", 0)

        if active == 0 and covered == 0:
            story.append(Paragraph(
                "Er zijn geen actieve contracten geregistreerd. Voeg contracten toe in de applicatie "
                "om de contractdekking te berekenen.",
                styles["BodyText2"],
            ))
            story.append(Spacer(1, 0.5 * cm))
            return

        story.append(Paragraph(
            f"De contractdekking geeft aan welk percentage van de be\u00efnvloedbare spend "
            f"gedekt wordt door een actief contract. Spend zonder contract (maverick spend) "
            f"vormt een risico en kan wijzen op ongecontroleerde inkoop.",
            styles["BodyText2"],
        ))
        story.append(Spacer(1, 0.3 * cm))

        summary = [
            ["Contractdekking", ""],
            ["Dekkingspercentage", f"{coverage_pct}%"],
            ["Gecontracteerde spend", self._fmt_eur(covered)],
            ["Maverick spend (zonder contract)", self._fmt_eur(maverick)],
            ["Aantal actieve contracten", str(active)],
        ]
        t = Table(summary, colWidths=[10 * cm, 5 * cm], repeatRows=1)
        t.setStyle(self._base_table_style(primary, len(summary), extra=[
            ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ]))
        story.append(t)
        story.append(Spacer(1, 0.3 * cm))

        if maverick > 0:
            story.append(Paragraph(
                f"\u2022 <b>{self._fmt_eur(maverick)}</b> aan uitgaven wordt gedaan zonder onderliggend "
                f"contract. Het is aan te bevelen deze spend te onderzoeken en waar mogelijk onder "
                f"contract te brengen.",
                styles["BulletText"],
            ))
        story.append(Spacer(1, 0.5 * cm))

        # Expiring contracts table
        if data["expiring_contracts"]:
            story.append(Paragraph("Aflopende contracten (binnen 2 jaar)", styles["SubTitle"]))
            header = ["Contract", "Einddatum", "Waarde", "Dagen resterend"]
            rows = [header]
            for c in data["expiring_contracts"][:10]:
                days_left = (c.end_date - date.today()).days if c.end_date else 0
                rows.append([
                    (c.name or "Onbekend")[:35],
                    c.end_date.strftime("%d-%m-%Y") if c.end_date else "-",
                    self._fmt_eur(float(c.estimated_value)) if c.estimated_value else "-",
                    str(days_left),
                ])
            t = Table(rows, colWidths=[7 * cm, 3 * cm, 3 * cm, 3 * cm], repeatRows=1)
            t.setStyle(self._base_table_style(primary, len(rows), extra=[
                ("ALIGN", (2, 0), (2, -1), "RIGHT"),
                ("ALIGN", (3, 0), (3, -1), "CENTER"),
            ]))
            story.append(t)
        story.append(Spacer(1, 0.5 * cm))

    # ── Section 9: Supplier Dynamics ─────────────────────────────────
    def _build_supplier_dynamics(self, story, styles, data, primary, secondary):
        story.append(Paragraph("9. Leveranciersdynamiek", styles["SectionTitle"]))

        dyn = data.get("supplier_dynamics")
        if not dyn:
            story.append(Paragraph(
                "Er is slechts data beschikbaar voor \u00e9\u00e9n jaar, "
                "waardoor geen leveranciersdynamiek getoond kan worden.",
                styles["BodyText2"],
            ))
            story.append(Spacer(1, 0.5 * cm))
            return

        story.append(Paragraph(
            f"Vergelijking van het leveranciersbestand in {data['year']} ten opzichte van "
            f"{dyn['compare_year']}. Nieuwe leveranciers zijn partijen die in {data['year']} "
            f"voor het eerst voorkomen; verloren leveranciers zijn partijen die in "
            f"{dyn['compare_year']} wel maar in {data['year']} niet meer voorkomen.",
            styles["BodyText2"],
        ))
        story.append(Spacer(1, 0.3 * cm))

        summary = [
            ["Leveranciersdynamiek", f"{data['year']} vs {dyn['compare_year']}"],
            ["Nieuwe leveranciers", f"{dyn['new_count']} ({self._fmt_eur(dyn['new_spend'])})"],
            ["Verloren leveranciers", f"{dyn['lost_count']} ({self._fmt_eur(dyn['lost_spend'])})"],
            ["Netto verschil", f"{dyn['new_count'] - dyn['lost_count']:+d} leveranciers"],
        ]
        t = Table(summary, colWidths=[8 * cm, 7 * cm], repeatRows=1)
        t.setStyle(self._base_table_style(primary, len(summary), extra=[
            ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ]))
        story.append(t)
        story.append(Spacer(1, 0.5 * cm))

    # ── Section 10: Category Growth ──────────────────────────────────
    def _build_category_growth(self, story, styles, data, primary, secondary, cat_label: str = "PIANOo"):
        story.append(Paragraph("10. Categorie-ontwikkeling", styles["SectionTitle"]))

        growth = data.get("category_growth", {})
        growers = growth.get("top_growers", [])
        decliners = growth.get("top_decliners", [])

        if not growers and not decliners:
            story.append(Paragraph(
                "Er is onvoldoende data beschikbaar om categorie-ontwikkeling te tonen "
                "(minimaal 2 jaar spenddata vereist).",
                styles["BodyText2"],
            ))
            story.append(Spacer(1, 0.5 * cm))
            return

        sorted_years = sorted(data["spend_map"].keys())
        story.append(Paragraph(
            f"Onderstaand overzicht toont de {cat_label}-categoriegroepen met de sterkste "
            f"spend-ontwikkeling tussen {sorted_years[-2]} en {sorted_years[-1]}.",
            styles["BodyText2"],
        ))
        story.append(Spacer(1, 0.3 * cm))

        if growers:
            story.append(Paragraph("Sterkste stijgers", styles["SubTitle"]))
            header = [f"{cat_label} Groep", f"Spend {sorted_years[-2]}", f"Spend {sorted_years[-1]}", "Verschil", "Groei %"]
            rows = [header]
            for g in growers:
                rows.append([
                    (g["groep"] or "Onbekend")[:30],
                    self._fmt_eur(g["previous_spend"]),
                    self._fmt_eur(g["current_spend"]),
                    f"+{self._fmt_eur(g['absolute_change'])}",
                    f"+{g['growth_pct']}%",
                ])
            t = Table(rows, colWidths=[5 * cm, 3 * cm, 3 * cm, 3 * cm, 2 * cm], repeatRows=1)
            t.setStyle(self._base_table_style(primary, len(rows), font_size=8, extra=[
                ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
                ("TEXTCOLOR", (4, 1), (4, -1), HexColor("#10b981")),
            ]))
            story.append(t)
            story.append(Spacer(1, 0.3 * cm))

        if decliners:
            story.append(Paragraph("Sterkste dalers", styles["SubTitle"]))
            header = [f"{cat_label} Groep", f"Spend {sorted_years[-2]}", f"Spend {sorted_years[-1]}", "Verschil", "Daling %"]
            rows = [header]
            for d in decliners:
                rows.append([
                    (d["groep"] or "Onbekend")[:30],
                    self._fmt_eur(d["previous_spend"]),
                    self._fmt_eur(d["current_spend"]),
                    self._fmt_eur(d["absolute_change"]),
                    f"{d['growth_pct']}%",
                ])
            t = Table(rows, colWidths=[5 * cm, 3 * cm, 3 * cm, 3 * cm, 2 * cm], repeatRows=1)
            t.setStyle(self._base_table_style(primary, len(rows), font_size=8, extra=[
                ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
                ("TEXTCOLOR", (4, 1), (4, -1), HexColor("#ef4444")),
            ]))
            story.append(t)
        story.append(Spacer(1, 0.5 * cm))

    # ── Section 11: Risk Analysis ────────────────────────────────────
    def _build_risk_analysis(self, story, styles, data, primary, secondary):
        story.append(Paragraph("11. Risicoanalyse", styles["SectionTitle"]))
        story.append(Paragraph(
            "De risicoanalyse toetst per inkooppakket of de geraamde opdrachtwaarde "
            "de Europese aanbestedingsdrempels overschrijdt.",
            styles["BodyText2"],
        ))
        story.append(Spacer(1, 0.3 * cm))

        # ── Risk pie chart ──
        pie_drawing = self._build_risk_pie(data)
        if pie_drawing:
            story.append(Paragraph("Risicoverdeling", styles["SubTitle"]))
            story.append(pie_drawing)
            story.append(Spacer(1, 0.3 * cm))

        # ── Risk summary table ──
        story.append(Paragraph("Overzicht per risiconiveau", styles["SubTitle"]))
        risk_rows = [["Risiconiveau", "Aantal", "Toelichting"]]
        risk_rows.append(["Aanbesteden", str(data["risk_summary"]["offertetraject"]),
                          "Boven drempel, geen aanbesteed contract"])
        risk_rows.append(["Onderzoek", str(data["risk_summary"]["meervoudig_onderhands"]),
                          "75-100% van drempel, nader onderzoek nodig"])
        risk_rows.append(["Monitoren", str(data["risk_summary"]["enkelvoudig_onderhands"]),
                          "Boven drempel, reeds aanbesteed contract"])
        risk_rows.append(["Akkoord", str(data["risk_summary"]["vrije_inkoop"]),
                          "Onder drempel"])

        t = Table(risk_rows, colWidths=[3 * cm, 2 * cm, 11 * cm], repeatRows=1)
        risk_style_extra = [("ALIGN", (1, 0), (1, -1), "CENTER")]
        for i, level in enumerate(["offertetraject", "meervoudig_onderhands", "enkelvoudig_onderhands", "vrije_inkoop"], 1):
            risk_style_extra.append(("TEXTCOLOR", (0, i), (0, i), RISK_COLORS[level]))
            risk_style_extra.append(("FONTNAME", (0, i), (0, i), "Helvetica-Bold"))
        t.setStyle(self._base_table_style(primary, len(risk_rows), extra=risk_style_extra))
        story.append(t)
        story.append(Spacer(1, 0.5 * cm))

        # ── Detail table (top 20) ──
        if data["risk_assessments"]:
            story.append(Paragraph("Detail per inkooppakket (top 20)", styles["SubTitle"]))
            detail_header = ["Categorie", "Spend", "Opdrachtwaarde", "Drempel", "%", "Niveau"]
            detail_rows = [detail_header]
            for r in data["risk_assessments"][:20]:
                cat = data["risk_categories"].get(r.category_id)
                cat_name = cat.inkooppakket[:30] if cat else str(r.category_id)
                pct = (
                    round(r.estimated_contract_value / r.internal_threshold * 100)
                    if r.internal_threshold else 0
                )
                detail_rows.append([
                    cat_name,
                    self._fmt_eur(r.yearly_spend),
                    self._fmt_eur(r.estimated_contract_value),
                    self._fmt_eur(r.internal_threshold),
                    f"{pct}%",
                    RISK_LABELS.get(r.risk_level, r.risk_level),
                ])

            t = Table(
                detail_rows,
                colWidths=[5 * cm, 2.5 * cm, 2.5 * cm, 2.5 * cm, 1.2 * cm, 2.5 * cm],
                repeatRows=1,
            )
            detail_extra = [("ALIGN", (1, 0), (4, -1), "RIGHT")]
            for i, r in enumerate(data["risk_assessments"][:20], 1):
                if r.risk_level in RISK_COLORS:
                    detail_extra.append(("TEXTCOLOR", (5, i), (5, i), RISK_COLORS[r.risk_level]))
                    detail_extra.append(("FONTNAME", (5, i), (5, i), "Helvetica-Bold"))
            t.setStyle(self._base_table_style(primary, len(detail_rows), font_size=7, extra=detail_extra))
            story.append(t)

    def _build_risk_pie(self, data):
        """Create a pie chart of risk distribution."""
        risk_data = []
        risk_labels = []
        risk_colors = []

        for level in ["offertetraject", "meervoudig_onderhands", "enkelvoudig_onderhands", "vrije_inkoop"]:
            count = data["risk_summary"][level]
            if count > 0:
                risk_data.append(count)
                risk_labels.append(f"{RISK_LABELS[level]} ({count})")
                risk_colors.append(RISK_COLORS[level])

        if not risk_data:
            return None

        d = Drawing(420, 180)
        pie = Pie()
        pie.x = 100
        pie.y = 10
        pie.width = 140
        pie.height = 140
        pie.data = risk_data
        pie.labels = risk_labels
        pie.sideLabels = True
        pie.simpleLabels = False
        pie.slices.fontName = "Helvetica"
        pie.slices.fontSize = 8
        for i, color in enumerate(risk_colors):
            pie.slices[i].fillColor = color
        d.add(pie)
        return d

    # ── Section 8: Recommendations ──────────────────────────────────────
    def _build_recommendations(self, story, styles, data, primary, secondary, accent):
        story.append(Paragraph("12. Aanbevelingen", styles["SectionTitle"]))
        story.append(Paragraph(
            "Op basis van de analyse worden de volgende acties aanbevolen:",
            styles["BodyText2"],
        ))
        story.append(Spacer(1, 0.3 * cm))

        recs: list[str] = []

        # From risk assessments
        for r in data["risk_assessments"]:
            cat = data["risk_categories"].get(r.category_id)
            cat_name = cat.inkooppakket if cat else str(r.category_id)
            if r.risk_level == "offertetraject":
                recs.append(
                    f"<b>Start aanbestedingsprocedure</b> voor {cat_name} "
                    f"(geraamde opdrachtwaarde: {self._fmt_eur(r.estimated_contract_value)})"
                )
            elif r.risk_level == "meervoudig_onderhands":
                recs.append(
                    f"<b>Onderzoek noodzaak aanbesteding</b> voor {cat_name} "
                    f"(75-100% van drempel)"
                )

        # From expiring contracts
        for c in data["expiring_contracts"][:5]:
            days_left = (c.end_date - date.today()).days if c.end_date else 0
            recs.append(
                f"<b>Contract loopt af:</b> {c.name} op {c.end_date.strftime('%d-%m-%Y')} "
                f"({days_left} dagen)"
            )

        if not recs:
            story.append(Paragraph(
                "Geen specifieke aanbevelingen op dit moment. Alle inkooppakketten vallen "
                "onder de aanbestedingsdrempels.",
                styles["BodyText2"],
            ))
        else:
            for i, rec in enumerate(recs[:15], 1):
                story.append(Paragraph(f"{i}. {rec}", styles["BulletText"]))

        story.append(Spacer(1, 1 * cm))

    # ── Section 9: Calendar Planning ────────────────────────────────────
    def _build_calendar_planning(self, story, styles, data, primary, secondary):
        story.append(Paragraph("13. WoCo Inkoopplatform", styles["SectionTitle"]))

        items = data["calendar_items"]
        if not items:
            story.append(Paragraph(
                "Er zijn nog geen kalenderitems gegenereerd. Gebruik de kalender-functie "
                "in de applicatie om een aanbestedingsplanning te genereren.",
                styles["BodyText2"],
            ))
            return

        story.append(Paragraph(
            f"Overzicht van {len(items)} geplande aanbestedingstrajecten.",
            styles["BodyText2"],
        ))
        story.append(Spacer(1, 0.3 * cm))

        header = ["Prioriteit", "Titel", "Waarde", "Start", "Publicatie", "Status"]
        rows = [header]
        priority_labels = {"high": "Hoog", "medium": "Midden", "low": "Laag"}
        status_labels = {
            "planned": "Gepland", "in_progress": "Bezig",
            "completed": "Afgerond", "cancelled": "Geannuleerd",
        }

        for item in items[:20]:
            rows.append([
                priority_labels.get(item.priority, item.priority),
                (item.title or "")[:35],
                self._fmt_eur(float(item.estimated_value)) if item.estimated_value else "-",
                item.target_start_date.strftime("%d-%m-%Y") if item.target_start_date else "-",
                item.target_publish_date.strftime("%d-%m-%Y") if item.target_publish_date else "-",
                status_labels.get(item.status, item.status),
            ])

        t = Table(
            rows,
            colWidths=[2 * cm, 5.5 * cm, 2.5 * cm, 2.5 * cm, 2.5 * cm, 2 * cm],
            repeatRows=1,
        )
        cal_extra = [("ALIGN", (2, 0), (2, -1), "RIGHT")]
        for i in range(1, len(rows)):
            prio = rows[i][0]
            if prio == "Hoog":
                cal_extra.append(("TEXTCOLOR", (0, i), (0, i), RISK_COLORS["offertetraject"]))
            elif prio == "Midden":
                cal_extra.append(("TEXTCOLOR", (0, i), (0, i), RISK_COLORS["meervoudig_onderhands"]))
            else:
                cal_extra.append(("TEXTCOLOR", (0, i), (0, i), RISK_COLORS["vrije_inkoop"]))
            cal_extra.append(("FONTNAME", (0, i), (0, i), "Helvetica-Bold"))
        t.setStyle(self._base_table_style(primary, len(rows), font_size=8, extra=cal_extra))
        story.append(t)
