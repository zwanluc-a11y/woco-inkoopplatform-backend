from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/referentie", tags=["referentie"])

CONTRACT_TYPES = [
    "Raamovereenkomst",
    "Onderhoudsovereenkomst",
    "Dienstverleningsovereenkomst",
    "Huurovereenkomst",
    "Leaseovereenkomst",
    "Overeenkomst",
    "Samenwerkingsovereenkomst",
    "Abonnement",
    "Eenmalige opdracht",
    "Beheerovereenkomst",
    "Garantie overeenkomst",
    "Intentieovereenkomst",
    "Koopovereenkomst",
    "Opdractbevestiging",
    "Personele inhuur",
    "RGS",
    "Sponsorovereenkomst",
    "Verwerkersovereenkomst",
    "Verzekeringsovereenkomst",
    "Anders",
]

CONTRACT_VORMEN = [
    "Individueel",
    "Samenwerking ZHZ",
    "Trevian Collectief",
    "Trevian Grip & Groei",
    "Trevian Inkoopmanagement",
    "Trevian Servicedesk",
]

LEVERANCIER_TYPES = [
    "Basis",
    "Contract",
    "Kern",
    "Strategisch",
]

CLASSIFICATIES = [
    "Basis",
    "Contract",
    "Kern",
    "Strategisch",
]

PDCA_STATUSSEN = [
    {"status": "Quickscan", "fase": "Plan"},
    {"status": "Startdocument", "fase": "Plan"},
    {"status": "Categorieplan", "fase": "Plan"},
    {"status": "Inkoopstrategie", "fase": "Do"},
    {"status": "Gunningsadvies", "fase": "Do"},
    {"status": "Implementatie", "fase": "Do"},
    {"status": "Contractadministratie", "fase": "Check"},
    {"status": "Contractmanagement", "fase": "Check"},
    {"status": "Act", "fase": "Act"},
]

GROOTTE_KLASSEN = [
    {"code": "XS", "label": "< 2.500 vhe's"},
    {"code": "S", "label": "2.501-5.000 vhe's"},
    {"code": "M", "label": "5.001-10.000 vhe's"},
    {"code": "L", "label": "10.001-25.000 vhe's"},
    {"code": "XL", "label": "> 25.000 vhe's"},
]


@router.get("")
async def get_all_reference_data():
    """Return all reference/lookup data in one call."""
    return {
        "contract_types": CONTRACT_TYPES,
        "contract_vormen": CONTRACT_VORMEN,
        "leverancier_types": LEVERANCIER_TYPES,
        "classificaties": CLASSIFICATIES,
        "pdca_statussen": PDCA_STATUSSEN,
        "grootte_klassen": GROOTTE_KLASSEN,
    }


@router.get("/contract-types")
async def get_contract_types():
    return CONTRACT_TYPES


@router.get("/contract-vormen")
async def get_contract_vormen():
    return CONTRACT_VORMEN


@router.get("/classificaties")
async def get_classificaties():
    return CLASSIFICATIES


@router.get("/pdca-statussen")
async def get_pdca_statussen():
    return PDCA_STATUSSEN
