"""
AI Recommendation Service.

Generates strategic procurement recommendations based on organization
overview data using Anthropic Claude API.
"""
from __future__ import annotations

import logging
from datetime import datetime

import anthropic
from sqlalchemy.orm import Session

from app.api.settings import get_anthropic_api_key
from app.models.organization import Organization

logger = logging.getLogger(__name__)

# Org type labels for prompt context
ORG_TYPE_LABELS = {
    "woningcorporatie_klein": "gemeenten",
    "woningcorporatie_groot": "waterschappen",
    "woningcorporatie_middel": "provincies",
    "woningcorporatie": "woningcorporaties",
    "rijksoverheid": "rijksoverheidsorganisaties",
    "zorg": "zorginstellingen",
    "onderwijs": "onderwijsinstellingen",
    "overig": "publieke organisaties",
}

CATEGORY_SYSTEM_LABELS = {
    "aedes": "PIANOo",
}


def _fmt(amount: float | int | None) -> str:
    """Format amount as €X.XXX for prompt readability."""
    if amount is None:
        return "€0"
    return f"€{amount:,.0f}".replace(",", ".")


def _build_data_summary(data: dict, org: Organization) -> str:
    """Build a structured text summary of overview data for the AI prompt."""
    lines: list[str] = []
    year = data.get("spend_year", datetime.now().year)

    # Basic spend info
    lines.append(f"## Organisatie: {org.name}")
    lines.append(f"Type: {org.org_type}")
    lines.append(f"Categoriesysteem: {CATEGORY_SYSTEM_LABELS.get(org.category_system, org.category_system)}")
    lines.append(f"Analysjaar: {year}")
    lines.append("")

    lines.append("## Spend overzicht")
    lines.append(f"- Totale spend {year}: {_fmt(data.get('total_spend_current_year'))}")
    prev = data.get("total_spend_previous_year", 0)
    if prev and prev > 0:
        lines.append(f"- Totale spend {year - 1}: {_fmt(prev)}")
        cur = data.get("total_spend_current_year", 0)
        if cur and prev:
            pct = ((cur - prev) / prev * 100)
            lines.append(f"- Jaar-op-jaar verandering: {pct:+.1f}%")
    lines.append(f"- Totale spend alle jaren: {_fmt(data.get('total_spend_all'))}")
    lines.append("")

    # Categorization status
    cat = data.get("categorization", {})
    if cat:
        lines.append("## Categorisatiestatus")
        lines.append(f"- {cat.get('categorized', 0)}/{cat.get('total', 0)} leveranciers gecategoriseerd ({cat.get('percentage', 0)}%)")
        if cat.get("uncategorized_spend", 0) > 0:
            lines.append(f"- Ongecategoriseerde spend: {_fmt(cat['uncategorized_spend'])}")
        lines.append("")

    # Spend per category group
    spend_by_groep = data.get("spend_by_groep", [])
    if spend_by_groep:
        lines.append("## Spend per categoriegroep")
        total = sum(g.get("total_spend", 0) for g in spend_by_groep)
        for g in sorted(spend_by_groep, key=lambda x: x.get("total_spend", 0), reverse=True):
            pct = (g["total_spend"] / total * 100) if total > 0 else 0
            lines.append(
                f"- {g['groep']}: {_fmt(g['total_spend'])} ({pct:.1f}%) — "
                f"{g.get('supplier_count', 0)} leveranciers"
            )
        lines.append("")

    # Top 10 suppliers
    top = data.get("top_suppliers", [])
    if top:
        lines.append("## Top 10 leveranciers")
        for i, s in enumerate(top[:10], 1):
            lines.append(f"  {i}. {s['name']}: {_fmt(s['total_spend'])}")
        lines.append("")

    # Pareto analysis
    pareto = data.get("pareto", {})
    if pareto and pareto.get("total_suppliers", 0) > 0:
        lines.append("## Concentratieanalyse (Pareto 80/20)")
        lines.append(
            f"- {pareto['suppliers_for_80_pct']} van {pareto['total_suppliers']} "
            f"leveranciers ({pareto.get('percentage', 0):.0f}%) zijn verantwoordelijk voor 80% van de spend"
        )
        lines.append("")

    # Risk summary
    risk = data.get("risk_summary", {})
    if risk:
        lines.append("## Risicoanalyse")
        for level, label in [("offertetraject", "Aanbesteden (boven drempel, geen contract)"),
                              ("meervoudig_onderhands", "Onderzoek (dicht bij drempel)"),
                              ("enkelvoudig_onderhands", "Monitoren (boven drempel, contract aanwezig)"),
                              ("vrije_inkoop", "Akkoord (onder drempel)")]:
            r = risk.get(level, {})
            if r.get("count", 0) > 0:
                lines.append(f"- {label}: {r['count']} categorieën, {_fmt(r.get('total_value', 0))}")
        above = data.get("above_threshold", {})
        if above and above.get("count", 0) > 0:
            lines.append(f"- Totaal boven drempel: {above['count']} categorieën, {_fmt(above.get('total_value', 0))}")
        lines.append("")

    # Contract coverage
    coverage = data.get("contract_coverage", {})
    has_contracts = coverage and coverage.get("coverage_pct", 0) > 0
    if has_contracts:
        lines.append("## Contractdekking")
        lines.append(f"- Dekkingspercentage: {coverage['coverage_pct']}%")
        lines.append(f"- Gecontracteerde spend: {_fmt(coverage.get('covered_spend', 0))}")
        if coverage.get("maverick_spend", 0) > 0:
            lines.append(f"- Maverick spend (zonder contract): {_fmt(coverage['maverick_spend'])}")
        lines.append("")

    # Expiring contracts
    expiring = data.get("expiring_contracts_list", [])
    if expiring:
        lines.append("## Aflopende contracten")
        for c in expiring[:15]:
            end = c.get("end_date", "onbekend")
            val = _fmt(c.get("estimated_value")) if c.get("estimated_value") else "onbekend"
            cat_name = c.get("category_name") or "geen categorie"
            lines.append(f"- {c['name']} (eindigt: {end}, waarde: {val}, categorie: {cat_name})")
        lines.append("")

    # Category growth
    growth = data.get("category_growth", {})
    growers = growth.get("top_growers", [])
    decliners = growth.get("top_decliners", [])
    if growers or decliners:
        lines.append("## Categorie-ontwikkeling")
        if growers:
            lines.append("Sterkste stijgers:")
            for g in growers[:5]:
                lines.append(
                    f"  - {g['category_name']} ({g['groep']}): "
                    f"{_fmt(g.get('previous_spend', 0))} → {_fmt(g.get('current_spend', 0))} "
                    f"({g.get('growth_pct', 0):+.1f}%)"
                )
        if decliners:
            lines.append("Sterkste dalers:")
            for d in decliners[:5]:
                lines.append(
                    f"  - {d['category_name']} ({d['groep']}): "
                    f"{_fmt(d.get('previous_spend', 0))} → {_fmt(d.get('current_spend', 0))} "
                    f"({d.get('growth_pct', 0):+.1f}%)"
                )
        lines.append("")

    # Supplier dynamics
    dynamics = data.get("supplier_dynamics")
    if dynamics:
        lines.append("## Leveranciersdynamiek")
        lines.append(f"- Nieuwe leveranciers: {dynamics['new_count']} ({_fmt(dynamics['new_spend'])} spend)")
        lines.append(f"- Verloren leveranciers: {dynamics['lost_count']} ({_fmt(dynamics['lost_spend'])} spend)")
        lines.append("")

    # Spend by procurement type
    by_type = data.get("spend_by_type", [])
    if by_type:
        lines.append("## Spend per inkoopsoort")
        for t in by_type:
            lines.append(f"- {t['type']}: {_fmt(t['total_spend'])} ({t.get('supplier_count', 0)} leveranciers)")
        lines.append("")

    # Niet-beïnvloedbaar
    nb = data.get("niet_beinvloedbaar", {})
    if nb and nb.get("total_spend", 0) > 0:
        lines.append("## Niet-beïnvloedbare spend")
        lines.append(f"- Totaal: {_fmt(nb['total_spend'])} ({nb.get('supplier_count', 0)} leveranciers)")
        lines.append("  (Deze spend is uitgesloten van bovenstaande analyses)")
        lines.append("")

    return "\n".join(lines)


def generate_recommendations(db: Session, org: Organization, overview_data: dict) -> str:
    """Generate AI procurement recommendations based on overview data."""
    api_key = get_anthropic_api_key(db)
    if not api_key:
        raise ValueError(
            "ANTHROPIC_API_KEY is niet geconfigureerd. "
            "Ga naar Instellingen om de API-sleutel in te stellen."
        )

    org_type_label = ORG_TYPE_LABELS.get(org.org_type, "publieke organisaties")
    cat_system_label = CATEGORY_SYSTEM_LABELS.get(
        org.category_system, org.category_system
    )

    # Determine if contracts are present
    coverage = overview_data.get("contract_coverage", {})
    has_contracts = bool(
        overview_data.get("expiring_contracts_list")
        or (coverage and coverage.get("coverage_pct", 0) > 0)
    )

    contract_section = ""
    if has_contracts:
        contract_section = """4. **Contractoptimalisatie** — aflopende contracten, dekkingsgaten, heronderhandelkansen
"""
    else:
        contract_section = """(Er zijn geen contracten geüpload — sla contractanalyse over en focus op spend-analyse.)
"""

    system_prompt = f"""Je bent een senior inkoopanalist en strategisch adviseur gespecialiseerd in inkoop voor Nederlandse {org_type_label}.
Je analyseert inkoopdata en geeft concrete, onderbouwde aanbevelingen.
Het categoriesysteem is {cat_system_label}.

Schrijf een helder, professioneel adviesrapport in het Nederlands met de volgende structuur:

1. **Samenvatting** — 2-3 zinnen met de kernboodschap en belangrijkste bevinding
2. **Bundelkansen** — welke categoriegroepen of leveranciers kunnen gebundeld worden voor schaalvoordeel? Let op: meerdere leveranciers in dezelfde categorie = bundelkans. Hoge concentratie = onderhandelkracht.
3. **Risico's en aandachtspunten** — drempeloverschrijdingen die actie vereisen, maverick spend, concentratierisico (Pareto), ongecategoriseerde spend
{contract_section}5. **Actiepunten** — top 3-5 geprioriteerde acties, elk met verwachte impact

Regels:
- Noem specifieke categorienamen, leveranciersnamen, bedragen en percentages uit de aangeleverde data
- Wees concreet en specifiek — elk advies moet direct terug te leiden zijn naar de data
- Schrijf beknopt maar onderbouwd (geen algemeenheden)
- Gebruik € voor bedragen
- Gebruik markdown formatting (bold, bullets, kopjes)
- Schrijf maximaal 800 woorden"""

    data_summary = _build_data_summary(overview_data, org)

    user_prompt = f"""Analyseer de volgende inkoopdata en genereer strategische aanbevelingen:

{data_summary}

Genereer nu het adviesrapport."""

    client = anthropic.Anthropic(api_key=api_key)

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

    return response.content[0].text.strip()
