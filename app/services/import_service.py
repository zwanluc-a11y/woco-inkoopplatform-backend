from __future__ import annotations
"""
Excel Import Service

Handles uploading, analyzing, and processing Excel files with transaction data
(boekingsregels) or existing spend analysis data.

Column detection uses heuristics on both column names and data content.
"""

import json
import logging
import re
import unicodedata
from datetime import date
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.models.contract import Contract, ContractSupplier
from app.models.import_session import ImportSession
from app.models.category import InkoopCategory
from app.models.supplier import Supplier
from app.models.supplier_yearly_spend import SupplierYearlySpend
from app.models.transaction import Transaction

logger = logging.getLogger(__name__)

# Heuristic patterns for column detection (Dutch accounting systems)
COLUMN_PATTERNS: dict[str, list[str]] = {
    "supplier_name": [
        "rekening", "leverancier", "crediteur", "naam", "creditor",
        "supplier", "debiteur/crediteur", "relatie",
    ],
    "supplier_code": [
        "rekeningnr", "crediteurcode", "crediteur nr", "leverancierscode",
        "code", "relatienr", "debiteur/crediteur",
    ],
    "amount": [
        "saldo", "bedrag", "amount", "totaal", "waarde", "netto",
        "bruto", "debet", "credit",
    ],
    "description": [
        "omschrijving", "beschrijving", "boekingsomschrijving",
        "factuuromschrijving", "tekst", "memo",
    ],
    "period": [
        "periode", "maand", "month", "period",
    ],
    "booking_date": [
        "boekstukdatum", "datum boeking", "factuurdatum", "boekdatum",
        "datum", "date", "invoicedate",
    ],
    "account_code": [
        "grootboek", "kostensoort", "gl account", "account",
    ],
    "cost_center": [
        "kostenplaats", "afdeling", "cost center", "department",
    ],
}


# Column patterns for contract register imports
CONTRACT_COLUMN_PATTERNS: dict[str, list[str]] = {
    "name": [
        "contractnaam", "contract naam", "naam contract", "contract name",
        "omschrijving contract", "beschrijving", "titel",
    ],
    "contract_number": [
        "contractnummer", "contract nummer", "contractnr", "contract nr",
        "referentienummer", "contract number", "nummer", "kenmerk",
    ],
    "contract_type": [
        "contracttype", "contract type", "type contract", "soort contract",
        "contractsoort", "type",
    ],
    "start_date": [
        "startdatum", "start datum", "ingangsdatum", "ingang", "begindatum",
        "start date", "aanvangsdatum",
    ],
    "end_date": [
        "einddatum", "eind datum", "expiratiedatum", "vervaldatum",
        "end date", "afloop", "einddatum contract",
    ],
    "auto_renewal": [
        "automatische verlenging", "auto verlenging", "stilzwijgend",
        "stilzwijgende verlenging", "auto renewal", "automatic renewal",
        "verlengingsoptie", "verlenging", "verlengingsopties",
        "extension", "verlengingsmogelijkheid",
    ],
    "max_end_date": [
        "maximale einddatum", "max einddatum", "uiterste einddatum",
        "max end date", "max. einddatum",
    ],
    "estimated_value": [
        "contractwaarde", "waarde", "geschatte waarde", "totaalwaarde",
        "bedrag", "budget", "estimated value", "contract value", "opdrachtwaarde",
    ],
    "is_ingekocht_via_procedure": [
        "aanbesteed", "is aanbesteed", "aanbesteding", "tender",
        "europees aanbesteed",
    ],
    "supplier_name": [
        "leverancier", "leveranciersnaam", "supplier", "contractpartij",
        "opdrachtnemer", "partij",
    ],
    "category_name": [
        "categorie", "inkooppakket", "aedes", "pianoo categorie",
        "inkoopcategorie", "category",
    ],
    "category_number": [
        "categorienummer", "pianoo nummer", "cat. nr", "cat nr",
        "category number",
    ],
    "notes": [
        "opmerkingen", "toelichting", "notities", "notes", "bijzonderheden",
    ],
}

CONTRACT_TYPE_MAP: dict[str, str] = {
    "raamcontract": "raamcontract", "raam": "raamcontract",
    "raamovereenkomst": "raamcontract",
    "huur": "huur_lease", "lease": "huur_lease", "huur/lease": "huur_lease",
    "onderhoud": "onderhoud", "maintenance": "onderhoud",
    "eenmalig": "eenmalig", "eenmalige opdracht": "eenmalig",
    "overig": "overig", "anders": "overig",
}


def _parse_currency(val: Any) -> Optional[float]:
    """Parse a currency value supporting NL and EN formats.

    Handles: -1234.56, -1.234,56, "- € 1.234,56", "1.234,56-", etc.
    """
    if pd.isna(val):
        return None

    # If it's already a number (float/int from pandas), return directly
    if isinstance(val, (int, float)):
        return float(val)

    s = str(val).strip()
    # Remove currency symbols and whitespace around them
    s = s.replace("€", "").replace("EUR", "").replace("eur", "").strip()
    # Remove any non-breaking spaces and regular spaces around minus
    s = s.replace("\u00a0", " ").strip()

    # Detect trailing minus: "1.234,56-" or "1234.56-"
    is_negative = False
    if s.endswith("-"):
        is_negative = True
        s = s[:-1].strip()
    elif s.startswith("-") or s.startswith("−"):  # regular or unicode minus
        is_negative = True
        s = s[1:].strip()

    # Remove any remaining spaces (e.g., "1 234,56")
    s = s.replace(" ", "")

    if not s:
        return None

    # Try as float directly
    try:
        result = float(s)
        return -result if is_negative else result
    except ValueError:
        pass
    # Try NL format: 150.000,00 → remove dots, replace comma with dot
    try:
        result = float(s.replace(".", "").replace(",", "."))
        return -result if is_negative else result
    except ValueError:
        return None


def _parse_boolean(val: Any) -> bool:
    """Parse a boolean-like value (ja/nee, true/false, 1/0)."""
    if pd.isna(val):
        return False
    s = str(val).strip().lower()
    return s in ("ja", "yes", "true", "1", "waar", "j", "y")


def _normalize_contract_type(val: Any) -> Optional[str]:
    """Normalize a contract type string to one of the allowed values."""
    if pd.isna(val):
        return None
    s = str(val).strip().lower()
    return CONTRACT_TYPE_MAP.get(s, "overig")


def _score_contract_column(col_name: str, data: pd.Series, field: str) -> float:
    """Score how likely a column matches a contract field."""
    score = 0.0
    col_lower = col_name.lower().strip()

    for pattern in CONTRACT_COLUMN_PATTERNS.get(field, []):
        if pattern in col_lower:
            score += 5.0
            if col_lower.startswith(pattern):
                score += 2.0
            break

    # Data type heuristics
    if field in ("start_date", "end_date", "max_end_date"):
        try:
            dates = pd.to_datetime(data.dropna().head(50), errors="coerce", dayfirst=True)
            if dates.notna().mean() > 0.5:
                score += 3.0
        except Exception:
            pass
    elif field == "estimated_value":
        try:
            cleaned = data.dropna().head(100).astype(str).str.replace(r"[€\s.,]", "", regex=True)
            numeric = pd.to_numeric(cleaned, errors="coerce")
            if numeric.notna().mean() > 0.5:
                score += 3.0
        except Exception:
            pass
    elif field == "is_ingekocht_via_procedure":
        if data.dtype == "object":
            vals = data.dropna().astype(str).str.lower().head(50)
            bool_vals = vals.isin(["ja", "nee", "yes", "no", "true", "false", "1", "0"])
            if bool_vals.mean() > 0.5:
                score += 4.0
    elif field == "supplier_name":
        if data.dtype == "object" and data.nunique() >= 2:
            score += 2.0
    elif field == "name":
        if data.dtype == "object":
            avg_len = data.dropna().astype(str).str.len().mean()
            if avg_len > 5:
                score += 2.0

    return score


def normalize_supplier_name(name: str) -> str:
    """Normalize a supplier name for deduplication matching."""
    if not name:
        return ""
    # Lowercase
    n = name.lower().strip()
    # Remove common Dutch and international business suffixes
    for suffix in [
        "b.v.", "bv", "n.v.", "nv", "v.o.f.", "vof", "c.v.", "cv",
        "inc.", "inc", "ltd.", "ltd", "gmbh", "ag", "sa", "s.a.",
    ]:
        n = n.replace(suffix, "")
    # Remove punctuation (dots, commas, dashes, parentheses, ampersands, slashes)
    n = re.sub(r"[.,\-()&/]", " ", n)
    # Normalize unicode
    n = unicodedata.normalize("NFKD", n)
    # Remove extra whitespace
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _score_column(col_name: str, data: pd.Series, field: str) -> float:
    """Score how likely a column matches a given field type."""
    score = 0.0
    col_lower = col_name.lower().strip()

    # Name matching
    for pattern in COLUMN_PATTERNS.get(field, []):
        if pattern in col_lower:
            score += 5.0
            if col_lower.startswith(pattern):
                score += 2.0
            break

    # Data type checks
    if field == "amount":
        try:
            numeric = pd.to_numeric(data.dropna().head(100), errors="coerce")
            ratio = numeric.notna().mean()
            if ratio > 0.7:
                score += 3.0
        except Exception:
            pass
    elif field == "booking_date":
        try:
            dates = pd.to_datetime(data.dropna().head(50), errors="coerce", dayfirst=True)
            ratio = dates.notna().mean()
            if ratio > 0.5:
                score += 3.0
        except Exception:
            pass
    elif field == "supplier_name":
        if data.dtype == "object":
            nunique = data.nunique()
            if 10 < nunique < len(data) * 0.8:
                score += 2.0
    elif field == "description":
        if data.dtype == "object":
            avg_len = data.dropna().astype(str).str.len().mean()
            if avg_len > 15:
                score += 2.0
    elif field == "period":
        try:
            numeric = pd.to_numeric(data.dropna().head(100), errors="coerce")
            if numeric.notna().mean() > 0.8:
                vals = numeric.dropna()
                if vals.min() >= 1 and vals.max() <= 12:
                    score += 4.0
        except Exception:
            pass

    return score


def _detect_header_row(
    df_raw: pd.DataFrame,
    patterns: Optional[dict[str, list[str]]] = None,
) -> int:
    """Try to detect which row contains the actual headers.

    Many Excel files have title rows or blank rows above the real headers.
    """
    if patterns is None:
        patterns = COLUMN_PATTERNS
    for i in range(min(10, len(df_raw))):
        row = df_raw.iloc[i]
        non_null = row.dropna()
        if len(non_null) >= 3:
            str_vals = non_null.astype(str).str.lower()
            matches = sum(
                1
                for v in str_vals
                for pat_list in patterns.values()
                for p in pat_list
                if p in v
            )
            if matches >= 2:
                return i
    return 0


class ImportService:
    def __init__(self, db: Session):
        self.db = db

    def analyze_file(
        self,
        file_path: str,
        file_name: str,
        org_id: int,
        user_id: int,
        import_type: Optional[str] = None,
    ) -> dict[str, Any]:
        """Read an uploaded Excel file, detect columns, suggest mapping, return preview."""
        path = Path(file_path)

        is_contract = import_type == "contract_register"
        col_patterns = CONTRACT_COLUMN_PATTERNS if is_contract else COLUMN_PATTERNS
        score_fn = _score_contract_column if is_contract else _score_column

        # Read with no header first to detect header row
        df_raw = pd.read_excel(path, header=None, nrows=15)
        header_row = _detect_header_row(df_raw, patterns=col_patterns)

        # Now read with correct header
        df = pd.read_excel(path, header=header_row)

        # Remove completely empty columns
        df = df.dropna(axis=1, how="all")

        # Ensure all column names are strings (pandas may read year cols as int)
        df.columns = [str(c) for c in df.columns]

        detected_columns = list(df.columns)

        # Score each column against each field
        suggested_mapping: dict[str, Optional[str]] = {}
        used_columns: set[str] = set()

        for field in col_patterns:
            best_col = None
            best_score = 0.0
            for col in detected_columns:
                if col in used_columns:
                    continue
                score = score_fn(col, df[col], field)
                if score > best_score:
                    best_score = score
                    best_col = col
            if best_col and best_score >= 3.0:
                suggested_mapping[field] = best_col
                used_columns.add(best_col)
            else:
                suggested_mapping[field] = None

        # Detect file type
        if is_contract:
            file_type = "contract_register"
        else:
            has_yearly_columns = any(
                re.match(r"^20\d{2}(?:\.0)?$", str(c).strip()) for c in detected_columns
            )
            file_type = "spend_analysis" if has_yearly_columns else "transactions"

        # Preview rows
        preview_df = df.head(10).fillna("")
        preview_rows = []
        for _, row in preview_df.iterrows():
            preview_rows.append(
                {str(k): _safe_json_value(v) for k, v in row.items()}
            )

        # Create import session
        session = ImportSession(
            organization_id=org_id,
            file_name=file_name,
            file_path=file_path,
            file_type=file_type,
            status="pending",
            column_mapping=suggested_mapping,
            uploaded_by=user_id,
        )
        self.db.add(session)
        self.db.commit()
        self.db.refresh(session)

        return {
            "id": session.id,
            "organization_id": org_id,
            "file_name": file_name,
            "file_type": file_type,
            "status": "pending",
            "detected_columns": detected_columns,
            "suggested_mapping": suggested_mapping,
            "preview_rows": preview_rows,
            "created_at": session.created_at,
        }

    def process_import(
        self,
        import_session: ImportSession,
        column_mapping: dict[str, str],
        year: Optional[int] = None,
    ) -> None:
        """Process the confirmed import: create suppliers and transactions."""
        import_session.status = "processing"
        import_session.column_mapping = column_mapping
        if year:
            import_session.year = year
        self.db.commit()

        # Read the file from stored path
        path = import_session.file_path
        if not path or not Path(path).exists():
            raise ValueError("Tijdelijk bestand niet gevonden. Upload het bestand opnieuw.")

        is_contract = import_session.file_type == "contract_register"
        patterns = CONTRACT_COLUMN_PATTERNS if is_contract else COLUMN_PATTERNS

        df_raw = pd.read_excel(path, header=None, nrows=15)
        header_row = _detect_header_row(df_raw, patterns=patterns)
        df = pd.read_excel(path, header=header_row)
        df = df.dropna(axis=1, how="all")
        df.columns = [str(c) for c in df.columns]

        org_id = import_session.organization_id

        # Set progress total
        import_session.progress_total = len(df)
        import_session.progress_current = 0
        self.db.commit()

        logger.info(
            "Processing import session %s: file_type=%s, columns=%s, mapping=%s",
            import_session.id, import_session.file_type, list(df.columns), column_mapping,
        )

        if is_contract:
            self._process_contract_register(df, column_mapping, org_id, import_session.id)
        elif import_session.file_type == "transactions":
            self._process_transactions(df, column_mapping, org_id, import_session.id, year, import_session)
        else:
            self._process_spend_analysis(df, column_mapping, org_id, import_session.id, import_session)

        import_session.row_count = len(df)
        import_session.progress_current = len(df)
        import_session.status = "completed"
        self.db.commit()

    def _process_transactions(
        self,
        df: pd.DataFrame,
        mapping: dict[str, str],
        org_id: int,
        session_id: int,
        year: Optional[int],
        import_session=None,
    ) -> None:
        """Process transaction-level data (boekingsregels)."""
        supplier_name_col = mapping.get("supplier_name")
        amount_col = mapping.get("amount")

        if not supplier_name_col or not amount_col:
            raise ValueError("Leveranciersnaam en bedrag kolommen zijn verplicht")

        desc_col = mapping.get("description")
        period_col = mapping.get("period")
        date_col = mapping.get("booking_date")
        code_col = mapping.get("supplier_code")
        account_col = mapping.get("account_code")
        cost_col = mapping.get("cost_center")

        # Detect if all amounts are negative (accounting convention)
        should_flip = self._detect_sign_flip(df, [amount_col])

        # Cache suppliers for this org — index by BOTH code and normalized name
        suppliers_by_code: dict[str, Supplier] = {}
        suppliers_by_name: dict[str, Supplier] = {}
        for s in self.db.query(Supplier).filter(Supplier.organization_id == org_id).all():
            if s.supplier_code:
                suppliers_by_code[s.supplier_code] = s
            suppliers_by_name[s.normalized_name] = s

        created = 0
        for idx, row in df.iterrows():
            name = str(row.get(supplier_name_col, "")).strip()
            if not name or name == "nan":
                continue

            amount_val = row.get(amount_col)
            amount = _parse_currency(amount_val)
            if amount is None:
                continue

            # Flip sign if accounting convention detected (× -1, not abs)
            if should_flip:
                amount = -amount

            supplier_code = None
            if code_col:
                raw_code = row.get(code_col)
                if pd.notna(raw_code):
                    supplier_code = str(int(raw_code)) if isinstance(raw_code, float) else str(raw_code)

            norm_name = normalize_supplier_name(name)

            # Find or create supplier — match on code first, then normalized name
            supplier = None
            if supplier_code:
                supplier = suppliers_by_code.get(supplier_code)
            if not supplier:
                supplier = suppliers_by_name.get(norm_name)
            if supplier:
                # Enrich: if we now have a code but supplier didn't, set it
                if supplier_code and not supplier.supplier_code:
                    supplier.supplier_code = supplier_code
                    suppliers_by_code[supplier_code] = supplier
            else:
                supplier = Supplier(
                    organization_id=org_id,
                    name=name,
                    supplier_code=supplier_code,
                    normalized_name=norm_name,
                )
                self.db.add(supplier)
                self.db.flush()
                if supplier_code:
                    suppliers_by_code[supplier_code] = supplier
                suppliers_by_name[norm_name] = supplier

            # Parse optional fields
            description = None
            if desc_col:
                d = row.get(desc_col)
                description = str(d).strip() if pd.notna(d) else None
            
            period = None
            if period_col:
                p = row.get(period_col)
                try:
                    period = int(p)
                except (ValueError, TypeError):
                    pass
            
            booking_date = None
            if date_col:
                bd = row.get(date_col)
                try:
                    val = pd.to_datetime(bd, dayfirst=True)
                    if pd.notna(val):
                        booking_date = val.date()
                except Exception:
                    pass

            tx_year = year
            if not tx_year and booking_date:
                tx_year = booking_date.year
            if not tx_year and period_col:
                # Try to get year from a 'Jaar' column if it exists
                jaar_val = row.get("Jaar") if "Jaar" in df.columns else None
                if pd.notna(jaar_val):
                    try:
                        tx_year = int(jaar_val)
                    except (ValueError, TypeError):
                        pass
            if not tx_year:
                tx_year = 2025  # fallback

            account_code = None
            if account_col:
                ac = row.get(account_col)
                account_code = str(ac).strip() if pd.notna(ac) else None
            
            cost_center = None
            if cost_col:
                cc = row.get(cost_col)
                cost_center = str(cc).strip() if pd.notna(cc) else None

            # Store raw data for traceability
            raw_data = {str(k): _safe_json_value(v) for k, v in row.items()}

            tx = Transaction(
                organization_id=org_id,
                supplier_id=supplier.id,
                import_session_id=session_id,
                year=tx_year,
                period=period,
                booking_date=booking_date,
                amount=amount,
                description=description,
                account_code=account_code,
                cost_center=cost_center,
                raw_data=raw_data,
            )
            self.db.add(tx)
            created += 1

            # Batch flush every 500 rows
            if created % 500 == 0:
                self.db.flush()

            # Chunked commit every 5000 rows — saves progress, data is safe
            if created % 5000 == 0:
                if import_session:
                    import_session.progress_current = created
                self.db.commit()

        self.db.flush()

        # Recalculate supplier yearly spend
        self._recalculate_yearly_spend(org_id)
        self.db.commit()
        logger.info("Imported %d transactions for org %d", created, org_id)

    @staticmethod
    def _detect_sign_flip(df: pd.DataFrame, amount_columns: list[str]) -> bool:
        """
        Detect if the majority of amounts are negative (accounting convention
        where expenses are shown as negative). If so, return True to indicate
        that all signs should be flipped (× -1).

        Rules:
        - If ≥90% of non-zero amounts are negative → flip (accounting convention)
          This handles the common case where expenses are negative and a few
          credits/refunds are positive.
        - Otherwise → don't flip
        """
        positive_count = 0
        negative_count = 0
        skipped_count = 0
        sample_values = []

        for col in amount_columns:
            for val in df[col]:
                if pd.isna(val):
                    continue
                num = _parse_currency(val)
                if num is None:
                    skipped_count += 1
                    if skipped_count <= 5:
                        logger.warning("_detect_sign_flip: could not parse value: %r (type=%s)", val, type(val).__name__)
                    continue
                if num == 0:
                    continue
                if len(sample_values) < 5:
                    sample_values.append((val, num))
                if num > 0:
                    positive_count += 1
                else:
                    negative_count += 1

        total = positive_count + negative_count
        negative_pct = (negative_count / total * 100) if total > 0 else 0

        logger.info(
            "_detect_sign_flip: total=%d (negative=%d, positive=%d, %.1f%% negative), "
            "skipped=%d, samples=%s",
            total, negative_count, positive_count, negative_pct,
            skipped_count, sample_values,
        )

        # If ≥90% of amounts are negative, it's an accounting convention
        if total > 0 and negative_pct >= 90:
            logger.info(
                "%.1f%% of %d amounts are negative — detected accounting convention, "
                "flipping ALL signs (× -1).", negative_pct, total,
            )
            return True
        return False

    def _process_spend_analysis(
        self,
        df: pd.DataFrame,
        mapping: dict[str, str],
        org_id: int,
        session_id: int,
        import_session=None,
    ) -> None:
        """Process aggregated spend analysis data (supplier + year columns)."""
        supplier_name_col = mapping.get("supplier_name")
        if not supplier_name_col:
            raise ValueError("Leveranciersnaam kolom is verplicht")

        # Detect year columns (columns named "2019", "2020", "2019.0", etc.)
        year_columns = [
            c for c in df.columns
            if re.match(r"^20\d{2}(?:\.0)?$", str(c).strip())
        ]

        if not year_columns:
            raise ValueError("Geen jaarkolommen gevonden (bijv. 2019, 2020, ...)")

        logger.info(
            "Spend analysis: year_columns=%s, rows=%d, columns=%s",
            year_columns, len(df), list(df.columns),
        )
        # Log sample raw values from first year column
        if year_columns:
            sample_raw = df[year_columns[0]].head(5).tolist()
            logger.info("Sample raw values from column '%s': %s (types: %s)",
                        year_columns[0], sample_raw, [type(v).__name__ for v in sample_raw])

        # Detect if all amounts are negative (accounting convention)
        should_flip = self._detect_sign_flip(df, year_columns)
        logger.info("Spend analysis: should_flip=%s", should_flip)

        existing_suppliers: dict[str, Supplier] = {}
        for s in self.db.query(Supplier).filter(Supplier.organization_id == org_id).all():
            existing_suppliers[s.normalized_name] = s

        # Track spend inserts to avoid unique constraint issues
        spend_cache: dict[tuple[int, int], SupplierYearlySpend] = {}
        # Pre-load existing spend records
        for ys in self.db.query(SupplierYearlySpend).filter(
            SupplierYearlySpend.organization_id == org_id
        ).all():
            spend_cache[(ys.supplier_id, ys.year)] = ys

        count = 0
        for _, row in df.iterrows():
            name = str(row.get(supplier_name_col, "")).strip()
            if not name or name == "nan":
                continue

            norm_name = normalize_supplier_name(name)
            supplier = existing_suppliers.get(norm_name)
            if not supplier:
                supplier = Supplier(
                    organization_id=org_id,
                    name=name,
                    normalized_name=norm_name,
                )
                self.db.add(supplier)
                self.db.flush()
                existing_suppliers[norm_name] = supplier

            for yc in year_columns:
                amount_val = row.get(yc)
                if pd.isna(amount_val):
                    continue
                amount = _parse_currency(amount_val)
                if amount is None or amount == 0:
                    continue

                # Flip sign if accounting convention detected
                if should_flip:
                    amount = abs(amount)

                yr = int(float(str(yc).strip()))
                key = (supplier.id, yr)

                # Upsert yearly spend using in-memory cache
                existing = spend_cache.get(key)
                if existing:
                    existing.total_amount = amount
                else:
                    ys = SupplierYearlySpend(
                        organization_id=org_id,
                        supplier_id=supplier.id,
                        year=yr,
                        total_amount=amount,
                        transaction_count=0,
                    )
                    self.db.add(ys)
                    spend_cache[key] = ys
                count += 1

            # Batch flush
            if count % 500 == 0:
                self.db.flush()

            # Chunked commit every 5000 rows
            if count % 5000 == 0:
                if import_session:
                    import_session.progress_current = count
                self.db.commit()

        self.db.commit()
        logger.info(
            "Imported spend analysis for org %d (%d records%s)",
            org_id, count, ", signs flipped" if should_flip else "",
        )

    def _process_contract_register(
        self,
        df: pd.DataFrame,
        mapping: dict[str, str],
        org_id: int,
        session_id: int,
    ) -> None:
        """Process a contract register import."""
        name_col = mapping.get("name")
        number_col = mapping.get("contract_number")
        type_col = mapping.get("contract_type")
        start_col = mapping.get("start_date")
        end_col = mapping.get("end_date")
        ext_col = mapping.get("auto_renewal")
        max_end_col = mapping.get("max_end_date")
        value_col = mapping.get("estimated_value")
        aanbesteed_col = mapping.get("is_ingekocht_via_procedure")
        supplier_col = mapping.get("supplier_name")
        cat_name_col = mapping.get("category_name")
        cat_nr_col = mapping.get("category_number")
        notes_col = mapping.get("notes")

        # Cache existing contracts by contract_number for deduplication
        existing_contracts: dict[str, Contract] = {}
        for c in self.db.query(Contract).filter(Contract.organization_id == org_id).all():
            if c.contract_number:
                existing_contracts[c.contract_number.strip().lower()] = c

        # Cache suppliers
        existing_suppliers: dict[str, Supplier] = {}
        for s in self.db.query(Supplier).filter(Supplier.organization_id == org_id).all():
            existing_suppliers[s.normalized_name] = s

        # Cache PIANOo categories
        all_categories = self.db.query(InkoopCategory).all()
        cat_by_name: dict[str, InkoopCategory] = {}
        cat_by_nr: dict[str, InkoopCategory] = {}
        for cat in all_categories:
            cat_by_name[cat.inkooppakket.lower()] = cat
            if cat.nummer:
                cat_by_nr[str(cat.nummer).strip()] = cat

        today = date.today()
        created = 0
        updated = 0

        for idx, row in df.iterrows():
            name = ""
            if name_col:
                name = str(row.get(name_col, "")).strip()
                if name == "nan":
                    name = ""

            # Parse contract number early — needed for fallback name
            contract_number = None
            if number_col:
                raw = row.get(number_col)
                if pd.notna(raw):
                    contract_number = str(raw).strip()
                    if contract_number.lower() == "nan":
                        contract_number = None

            # Fallback name: use contract number, then supplier, then row number
            if not name:
                if contract_number:
                    name = f"Contract {contract_number}"
                elif supplier_col:
                    raw_sup = row.get(supplier_col)
                    if pd.notna(raw_sup) and str(raw_sup).strip() and str(raw_sup).strip() != "nan":
                        name = f"Contract {str(raw_sup).strip()}"
                if not name:
                    name = f"Contract {created + updated + 1}"

            # Check for existing contract (dedup on contract_number)
            existing = None
            if contract_number:
                existing = existing_contracts.get(contract_number.lower())

            # Parse all fields
            contract_type = None
            if type_col:
                contract_type = _normalize_contract_type(row.get(type_col))

            start_date = None
            if start_col:
                try:
                    val = pd.to_datetime(row.get(start_col), dayfirst=True)
                    if pd.notna(val):
                        start_date = val.date()
                except Exception:
                    pass

            end_date = None
            if end_col:
                try:
                    val = pd.to_datetime(row.get(end_col), dayfirst=True)
                    if pd.notna(val):
                        end_date = val.date()
                except Exception:
                    pass

            auto_renewal = False
            if ext_col:
                auto_renewal = _parse_boolean(row.get(ext_col))

            max_end_date = None
            if max_end_col:
                try:
                    val = pd.to_datetime(row.get(max_end_col), dayfirst=True)
                    if pd.notna(val):
                        max_end_date = val.date()
                except Exception:
                    pass

            estimated_value = None
            if value_col:
                estimated_value = _parse_currency(row.get(value_col))

            is_ingekocht_via_procedure = False
            if aanbesteed_col:
                is_ingekocht_via_procedure = _parse_boolean(row.get(aanbesteed_col))

            notes = None
            if notes_col:
                raw = row.get(notes_col)
                if pd.notna(raw):
                    notes = str(raw).strip()

            # Determine status based on dates
            status = "active"
            if end_date:
                if end_date < today:
                    status = "expired"
                elif (end_date - today).days <= 180:
                    status = "expiring"
            if start_date and start_date > today:
                status = "planned"

            # Find PIANOo category
            category_id = None
            if cat_nr_col:
                raw_nr = row.get(cat_nr_col)
                if pd.notna(raw_nr):
                    cat = cat_by_nr.get(str(raw_nr).strip())
                    if cat:
                        category_id = cat.id
            if not category_id and cat_name_col:
                raw_name = row.get(cat_name_col)
                if pd.notna(raw_name):
                    search = str(raw_name).strip().lower()
                    # Try exact match first, then partial
                    cat = cat_by_name.get(search)
                    if not cat:
                        for k, v in cat_by_name.items():
                            if search in k or k in search:
                                cat = v
                                break
                    if cat:
                        category_id = cat.id

            if existing:
                # Update existing contract
                existing.name = name
                existing.contract_type = contract_type
                existing.start_date = start_date
                existing.end_date = end_date
                existing.auto_renewal = auto_renewal
                existing.max_end_date = max_end_date
                existing.estimated_value = estimated_value
                existing.is_ingekocht_via_procedure = is_ingekocht_via_procedure
                existing.status = status
                existing.category_id = category_id
                existing.notes = notes
                existing.import_session_id = session_id
                contract = existing
                updated += 1
            else:
                contract = Contract(
                    organization_id=org_id,
                    name=name,
                    contract_number=contract_number,
                    contract_type=contract_type,
                    start_date=start_date,
                    end_date=end_date,
                    auto_renewal=auto_renewal,
                    max_end_date=max_end_date,
                    estimated_value=estimated_value,
                    is_ingekocht_via_procedure=is_ingekocht_via_procedure,
                    status=status,
                    category_id=category_id,
                    notes=notes,
                    import_session_id=session_id,
                )
                self.db.add(contract)
                if contract_number:
                    existing_contracts[contract_number.lower()] = contract
                created += 1

            self.db.flush()

            # Link supplier via ContractSupplier
            if supplier_col:
                raw_supplier = row.get(supplier_col)
                if pd.notna(raw_supplier):
                    sup_name = str(raw_supplier).strip()
                    if sup_name and sup_name != "nan":
                        norm = normalize_supplier_name(sup_name)
                        supplier = existing_suppliers.get(norm)

                        # Fuzzy match against existing suppliers if no exact match
                        if not supplier and existing_suppliers:
                            try:
                                from rapidfuzz import fuzz, process as rfprocess
                                match = rfprocess.extractOne(
                                    norm,
                                    existing_suppliers.keys(),
                                    scorer=fuzz.ratio,
                                    score_cutoff=85,
                                )
                                if match:
                                    matched_name, score, _ = match
                                    supplier = existing_suppliers[matched_name]
                                    logger.info(
                                        "Fuzzy matched supplier '%s' → '%s' (score=%.0f%%)",
                                        sup_name, supplier.name, score,
                                    )
                            except ImportError:
                                pass  # rapidfuzz not installed, skip fuzzy matching

                        if not supplier:
                            supplier = Supplier(
                                organization_id=org_id,
                                name=sup_name,
                                normalized_name=norm,
                            )
                            self.db.add(supplier)
                            self.db.flush()
                            existing_suppliers[norm] = supplier

                        # 1 leverancier per contract — vervang bestaande link
                        self.db.query(ContractSupplier).filter(
                            ContractSupplier.contract_id == contract.id,
                        ).delete()
                        self.db.add(ContractSupplier(
                            contract_id=contract.id,
                            supplier_id=supplier.id,
                        ))

        self.db.commit()
        logger.info(
            "Imported contract register for org %d: %d created, %d updated",
            org_id, created, updated,
        )

    def _recalculate_yearly_spend(self, org_id: int) -> None:
        """Recalculate supplier_yearly_spend from transactions."""
        from sqlalchemy import func

        # Delete existing aggregations for this org
        self.db.query(SupplierYearlySpend).filter(
            SupplierYearlySpend.organization_id == org_id
        ).delete()

        # Aggregate from transactions
        rows = (
            self.db.query(
                Transaction.supplier_id,
                Transaction.year,
                func.sum(Transaction.amount).label("total"),
                func.count(Transaction.id).label("cnt"),
            )
            .filter(Transaction.organization_id == org_id)
            .group_by(Transaction.supplier_id, Transaction.year)
            .all()
        )

        for r in rows:
            ys = SupplierYearlySpend(
                organization_id=org_id,
                supplier_id=r.supplier_id,
                year=r.year,
                total_amount=float(r.total),
                transaction_count=r.cnt,
            )
            self.db.add(ys)

    # _find_temp_file removed — we now store file_path directly on ImportSession


def _safe_json_value(val: Any) -> Any:
    """Convert a pandas value to something JSON-serializable."""
    if pd.isna(val):
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    if isinstance(val, (int, float, str, bool)):
        return val
    return str(val)
