"""Heuristic classifier that assigns 'Werken' / 'Leveringen' / 'Diensten' to a category.

Based on the Aanbestedingswet:
- Werken: construction-style work (build, renovate, demolish, paint, repair structure)
- Leveringen: physical goods (energy, fuel, vehicles, supplies, equipment)
- Diensten: everything else (services, consulting, maintenance, cleaning)
"""
from __future__ import annotations

from typing import Iterable

from sqlalchemy.orm import Session

from app.models.category import InkoopCategory


WERKEN_KEYWORDS: tuple[str, ...] = (
    "bouw", "renovat", "sloop", "verbouw", "beton", "metsel", "timmer",
    "schilder", "voeg", "dakwerk", "dakonderhoud", "dakgoten", "dakgotenreiniging",
    "kozijn", "gevel", "vloer", "tegel", "asfalt", "bestrating", "aannemer",
    "nieuwbouw", "duurzaamheid aannemer", "milieu/verontreiniging aannemer",
    "hang en sluitwerk", "isolatie", "mo en do", "mjop", "dagelijks onderhoud",
    "gebouwbeheer", "glas",
)

LEVERINGEN_KEYWORDS: tuple[str, ...] = (
    "energie", "gas", "elektra", "stroom", "brandstof", "voertuig", "auto",
    "kantoorartikel", "meubilair", "meubel", "apparatuur", "gereedschap",
    "kleding", "bedrijfskleding", "meetapparatuur", "av middel", "av middelen",
    "telefoon", "hardware", "pc", "laptop", "printer", "papier", "drukwerk",
    "automatische deuren",
)


def classify_soort_inkoop(name: str, groep: str | None = None) -> str:
    """Classify a category as Werken, Leveringen, or Diensten.

    Heuristic order:
      1. WERKEN: keyword match in name (construction terms).
      2. LEVERINGEN: keyword match in name (physical goods).
      3. Group-based fallback: 7-Energie → Leveringen.
      4. Group-based fallback: 1-Vastgoed → Werken (mostly construction).
      5. Default: Diensten.
    """
    name_lower = (name or "").lower()
    g = (groep or "").strip()

    for kw in WERKEN_KEYWORDS:
        if kw in name_lower:
            return "Werken"

    for kw in LEVERINGEN_KEYWORDS:
        if kw in name_lower:
            return "Leveringen"

    if g == "7-Energie":
        return "Leveringen"
    if g == "1-Vastgoed":
        return "Werken"

    return "Diensten"


def backfill_soort_inkoop(db: Session, force: bool = False) -> dict:
    """Populate InkoopCategory.soort_inkoop where it's empty (or all if force=True)."""
    query = db.query(InkoopCategory)
    if not force:
        query = query.filter(
            (InkoopCategory.soort_inkoop == None)  # noqa: E711
            | (InkoopCategory.soort_inkoop == "")
        )

    rows = query.all()
    counts: dict[str, int] = {"Werken": 0, "Leveringen": 0, "Diensten": 0}
    for cat in rows:
        soort = classify_soort_inkoop(cat.inkooppakket, cat.groep)
        cat.soort_inkoop = soort
        counts[soort] += 1

    db.commit()
    return {"updated": len(rows), "by_soort": counts}
