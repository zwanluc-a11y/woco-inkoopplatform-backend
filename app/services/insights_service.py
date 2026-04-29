"""Shared insights data fetching service.

Used by:
- API endpoints (departments.py, spend.py)
- PDF report service (pdf_service.py)

Keeps API and PDF in sync — single source of truth for analytics aggregations.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Optional

from sqlalchemy.orm import Session

from app.models.category import InkoopCategory
from app.models.category_department import CategoryDepartment
from app.models.contract import Contract, ContractSupplier
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_yearly_spend import SupplierYearlySpend


class InsightsService:
    """Aggregated analytics for an organization."""

    def __init__(self, db: Session):
        self.db = db

    # ── Department insights ──────────────────────────────────────────────
    def get_department_insights(self, org_id: int) -> dict:
        """Comprehensive insights per department: spend, suppliers, contracts, top categories, soort_inkoop."""
        cat_dept_rows = (
            self.db.query(CategoryDepartment.category_id, CategoryDepartment.afdeling)
            .filter(CategoryDepartment.organization_id == org_id)
            .all()
        )
        cat_to_afd: dict[int, str] = {r.category_id: r.afdeling for r in cat_dept_rows}

        if not cat_to_afd:
            return {
                "departments": [],
                "totals": {
                    "total_spend": 0,
                    "total_suppliers": 0,
                    "total_contracts": 0,
                    "total_categories_mapped": 0,
                    "spend_by_soort": {"Werken": 0, "Leveringen": 0, "Diensten": 0},
                },
                "unmapped": {"supplier_count": 0, "total_spend": 0},
                "all_years": [],
            }

        categorizations = (
            self.db.query(SupplierCategorization)
            .filter(SupplierCategorization.organization_id == org_id)
            .all()
        )

        supplier_dept_weights: dict[int, list[tuple[str, float, int]]] = defaultdict(list)
        for sc in categorizations:
            afd = cat_to_afd.get(sc.category_id)
            if afd:
                supplier_dept_weights[sc.supplier_id].append(
                    (afd, sc.percentage / 100.0, sc.category_id)
                )

        yearly_spends = (
            self.db.query(SupplierYearlySpend)
            .filter(SupplierYearlySpend.organization_id == org_id)
            .all()
        )

        all_cats = self.db.query(InkoopCategory).all()
        category_names = {c.id: c.inkooppakket for c in all_cats}
        category_soort = {c.id: (c.soort_inkoop or "Diensten") for c in all_cats}

        contracts = self.db.query(Contract).filter(Contract.organization_id == org_id).all()
        contract_supplier_links = (
            self.db.query(ContractSupplier.contract_id, ContractSupplier.supplier_id)
            .join(Contract, Contract.id == ContractSupplier.contract_id)
            .filter(Contract.organization_id == org_id)
            .all()
        )
        contract_to_suppliers: dict[int, list[int]] = defaultdict(list)
        for r in contract_supplier_links:
            contract_to_suppliers[r.contract_id].append(r.supplier_id)

        dept_data: dict[str, dict] = defaultdict(lambda: {
            "name": "",
            "category_ids": set(),
            "supplier_ids": set(),
            "contract_ids": set(),
            "total_spend": 0.0,
            "spend_per_year": defaultdict(float),
            "transactions_per_year": defaultdict(int),
            "category_spend": defaultdict(float),
            "category_supplier_count": defaultdict(set),
            "spend_by_soort": defaultdict(float),
        })
        all_years: set[int] = set()
        org_spend_by_soort: dict[str, float] = defaultdict(float)

        for ys in yearly_spends:
            weights = supplier_dept_weights.get(ys.supplier_id, [])
            if not weights:
                continue
            all_years.add(ys.year)
            per_dept: dict[str, float] = defaultdict(float)
            per_dept_cats: dict[str, list[int]] = defaultdict(list)
            for afd, pct, cat_id in weights:
                per_dept[afd] += pct
                per_dept_cats[afd].append(cat_id)
            for afd, weight in per_dept.items():
                d = dept_data[afd]
                d["name"] = afd
                d["supplier_ids"].add(ys.supplier_id)
                spend = float(ys.total_amount) * weight
                d["total_spend"] += spend
                d["spend_per_year"][ys.year] += spend
                d["transactions_per_year"][ys.year] += int((ys.transaction_count or 0) * weight)
                for cat_id in per_dept_cats[afd]:
                    d["category_ids"].add(cat_id)
                    for sc_afd, sc_pct, sc_cat in weights:
                        if sc_cat == cat_id and sc_afd == afd:
                            cat_spend = float(ys.total_amount) * sc_pct
                            d["category_spend"][cat_id] += cat_spend
                            d["category_supplier_count"][cat_id].add(ys.supplier_id)
                            soort = category_soort.get(cat_id, "Diensten")
                            d["spend_by_soort"][soort] += cat_spend
                            org_spend_by_soort[soort] += cat_spend
                            break

        for c in contracts:
            sup_ids = contract_to_suppliers.get(c.id, [])
            depts_for_contract: set[str] = set()
            for sid in sup_ids:
                for afd, _, _ in supplier_dept_weights.get(sid, []):
                    depts_for_contract.add(afd)
            for afd in depts_for_contract:
                dept_data[afd]["contract_ids"].add(c.id)

        departments_out = []
        for afd, d in dept_data.items():
            top_cats = sorted(
                d["category_spend"].items(), key=lambda x: abs(x[1]), reverse=True
            )[:5]
            top_cats_out = [
                {
                    "category_id": cid,
                    "category_name": category_names.get(cid, "?"),
                    "spend": round(spend, 2),
                    "supplier_count": len(d["category_supplier_count"][cid]),
                }
                for cid, spend in top_cats
            ]
            years_sorted = sorted(d["spend_per_year"].keys())
            spend_per_year = [
                {
                    "year": y,
                    "spend": round(d["spend_per_year"][y], 2),
                    "transactions": d["transactions_per_year"][y],
                }
                for y in years_sorted
            ]
            soort_breakdown = {
                soort: round(d["spend_by_soort"].get(soort, 0.0), 2)
                for soort in ("Werken", "Leveringen", "Diensten")
            }
            departments_out.append({
                "name": afd,
                "category_count": len(d["category_ids"]),
                "supplier_count": len(d["supplier_ids"]),
                "contract_count": len(d["contract_ids"]),
                "total_spend": round(d["total_spend"], 2),
                "spend_per_year": spend_per_year,
                "top_categories": top_cats_out,
                "spend_by_soort": soort_breakdown,
            })

        departments_out.sort(key=lambda x: x["total_spend"], reverse=True)

        total_spend = sum(d["total_spend"] for d in departments_out)
        total_suppliers_unique = (
            len(set().union(*[dept_data[d]["supplier_ids"] for d in dept_data])) if dept_data else 0
        )
        total_contracts_unique = (
            len(set().union(*[dept_data[d]["contract_ids"] for d in dept_data])) if dept_data else 0
        )

        mapped_supplier_ids = (
            set().union(*[dept_data[d]["supplier_ids"] for d in dept_data]) if dept_data else set()
        )
        unmapped_spend = 0.0
        unmapped_suppliers: set[int] = set()
        for ys in yearly_spends:
            if ys.supplier_id not in mapped_supplier_ids:
                unmapped_spend += float(ys.total_amount)
                unmapped_suppliers.add(ys.supplier_id)

        return {
            "departments": departments_out,
            "totals": {
                "total_spend": round(total_spend, 2),
                "total_suppliers": total_suppliers_unique,
                "total_contracts": total_contracts_unique,
                "total_categories_mapped": len(cat_to_afd),
                "spend_by_soort": {
                    soort: round(org_spend_by_soort.get(soort, 0.0), 2)
                    for soort in ("Werken", "Leveringen", "Diensten")
                },
            },
            "unmapped": {
                "supplier_count": len(unmapped_suppliers),
                "total_spend": round(unmapped_spend, 2),
            },
            "all_years": sorted(all_years),
        }

    # ── Spend insights ───────────────────────────────────────────────────
    def get_spend_insights(self, org_id: int) -> dict:
        """Per-supplier transaction analytics: avg invoice value, transaction counts, top performers."""
        suppliers = (
            self.db.query(Supplier)
            .filter(Supplier.organization_id == org_id)
            .all()
        )

        all_years: set[int] = set()
        rows = []
        year_agg: dict[int, dict] = defaultdict(lambda: {
            "total_spend": 0.0,
            "transaction_count": 0,
            "supplier_count": 0,
        })

        for s in suppliers:
            yearly: dict[str, dict] = {}
            total_spend = 0.0
            total_transactions = 0
            for ys in s.yearly_spends:
                yr = ys.year
                all_years.add(yr)
                amt = float(ys.total_amount)
                tc = ys.transaction_count or 0
                avg = amt / tc if tc > 0 else 0.0
                yearly[str(yr)] = {
                    "spend": amt,
                    "transaction_count": tc,
                    "avg_invoice": round(avg, 2),
                }
                total_spend += amt
                total_transactions += tc
                year_agg[yr]["total_spend"] += amt
                year_agg[yr]["transaction_count"] += tc
                year_agg[yr]["supplier_count"] += 1

            if total_transactions == 0 and total_spend == 0:
                continue

            overall_avg = total_spend / total_transactions if total_transactions > 0 else 0.0
            cats = s.categorizations or []
            primary = max(cats, key=lambda c: c.percentage, default=None) if cats else None

            rows.append({
                "id": s.id,
                "name": s.name,
                "supplier_code": s.supplier_code,
                "category_name": primary.category.inkooppakket if primary and primary.category else None,
                "is_beinvloedbaar": s.is_beinvloedbaar,
                "yearly": yearly,
                "total_spend": total_spend,
                "total_transactions": total_transactions,
                "avg_invoice": round(overall_avg, 2),
            })

        years_sorted = sorted(all_years)
        year_summaries = []
        for yr in years_sorted:
            agg = year_agg[yr]
            avg = agg["total_spend"] / agg["transaction_count"] if agg["transaction_count"] > 0 else 0.0
            year_summaries.append({
                "year": yr,
                "total_spend": round(agg["total_spend"], 2),
                "transaction_count": agg["transaction_count"],
                "supplier_count": agg["supplier_count"],
                "avg_invoice": round(avg, 2),
            })

        grand_spend = sum(r["total_spend"] for r in rows)
        grand_transactions = sum(r["total_transactions"] for r in rows)
        grand_avg = grand_spend / grand_transactions if grand_transactions > 0 else 0.0

        meaningful = [r for r in rows if r["total_transactions"] >= 3]
        meaningful.sort(key=lambda x: x["avg_invoice"], reverse=True)
        top_avg = meaningful[:5]
        most_transactions = sorted(rows, key=lambda x: x["total_transactions"], reverse=True)[:5]

        return {
            "total_count": len(rows),
            "suppliers_all": rows,  # kept for PDF; API paginates separately
            "year_summaries": year_summaries,
            "totals": {
                "total_spend": round(grand_spend, 2),
                "total_transactions": grand_transactions,
                "avg_invoice": round(grand_avg, 2),
                "supplier_count": len(rows),
            },
            "top_avg_invoice": top_avg,
            "top_transactions": most_transactions,
        }

    # ── Category pivot ───────────────────────────────────────────────────
    def get_category_pivot(self, org_id: int, top_n: Optional[int] = None) -> dict:
        """Categories with their suppliers nested (weighted by categorization %).

        If top_n is given, only the top-N categories by total spend are returned,
        with at most 3 suppliers each (used for compact PDF rendering).
        """
        suppliers = (
            self.db.query(Supplier)
            .filter(Supplier.organization_id == org_id)
            .all()
        )

        contract_supplier_rows = (
            self.db.query(ContractSupplier.supplier_id, Contract.name)
            .join(Contract, Contract.id == ContractSupplier.contract_id)
            .filter(Contract.organization_id == org_id)
            .all()
        )
        supplier_contracts: dict[int, list[str]] = {}
        for row in contract_supplier_rows:
            supplier_contracts.setdefault(row.supplier_id, []).append(row.name)

        categories_map: dict[int | None, dict] = {}

        for s in suppliers:
            spends: dict[str, float] = {}
            total = 0.0
            for ys in s.yearly_spends:
                spends[str(ys.year)] = float(ys.total_amount)
                total += float(ys.total_amount)

            contracts_for_supplier = supplier_contracts.get(s.id, [])
            supplier_data = {
                "id": s.id,
                "name": s.name,
                "supplier_code": s.supplier_code,
                "is_beinvloedbaar": s.is_beinvloedbaar,
                "has_contract": len(contracts_for_supplier) > 0,
                "contract_names": contracts_for_supplier,
                "spends": spends,
                "total": total,
            }

            cats = s.categorizations or []
            if not cats:
                cat_key = None
                if cat_key not in categories_map:
                    categories_map[cat_key] = {
                        "category_id": None,
                        "category_name": "Ongecategoriseerd",
                        "suppliers": [],
                        "spends": defaultdict(float),
                        "total": 0.0,
                        "supplier_count": 0,
                    }
                cat = categories_map[cat_key]
                cat["suppliers"].append(supplier_data)
                cat["supplier_count"] += 1
                cat["total"] += total
                for yr, amt in spends.items():
                    cat["spends"][yr] += amt
            else:
                for sc in cats:
                    cat_id = sc.category_id
                    cat_name = sc.category.inkooppakket if sc.category else "Onbekend"
                    pct = sc.percentage / 100.0
                    if cat_id not in categories_map:
                        categories_map[cat_id] = {
                            "category_id": cat_id,
                            "category_name": cat_name,
                            "suppliers": [],
                            "spends": defaultdict(float),
                            "total": 0.0,
                            "supplier_count": 0,
                        }
                    cat = categories_map[cat_id]
                    weighted_spends = {yr: amt * pct for yr, amt in spends.items()}
                    weighted_total = total * pct
                    cat["suppliers"].append({
                        **supplier_data,
                        "spends": weighted_spends,
                        "total": weighted_total,
                        "percentage": sc.percentage,
                    })
                    cat["supplier_count"] += 1
                    cat["total"] += weighted_total
                    for yr, amt in weighted_spends.items():
                        cat["spends"][yr] += amt

        result = []
        for cat in categories_map.values():
            cat["suppliers"].sort(key=lambda x: abs(x["total"]), reverse=True)
            limited_suppliers = cat["suppliers"][:3] if top_n else cat["suppliers"]
            result.append({
                "category_id": cat["category_id"],
                "category_name": cat["category_name"],
                "supplier_count": cat["supplier_count"],
                "spends": dict(cat["spends"]),
                "total": cat["total"],
                "suppliers": limited_suppliers,
            })

        result.sort(key=lambda x: abs(x["total"]), reverse=True)
        if top_n:
            result = result[:top_n]

        return {"categories": result, "total_count": len(result)}
