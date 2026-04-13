from __future__ import annotations

from dataclasses import dataclass
from typing import Any

ACCOUNT_ID_MAP: dict[str, str] = {
    "ifrs-full_Revenue": "revenue",
    "ifrs-full_CostOfSales": "cogs",
    "ifrs-full_GrossProfit": "gross_profit",
    "dart_TotalSellingGeneralAdministrativeExpenses": "sga",
    "dart_OperatingIncomeLoss": "operating_income",
    "ifrs-full_InterestExpense": "interest_expense",
    "ifrs-full_ProfitLoss": "net_income",
    "ifrs-full_Assets": "total_assets",
    "ifrs-full_CashAndCashEquivalents": "cash_and_equivalents",
    "ifrs-full_Equity": "total_equity",
    "ifrs-full_EquityAttributableToOwnersOfParent": "total_equity",
    "ifrs-full_Liabilities": "total_liabilities",
    "ifrs-full_DepreciationAndAmortisationExpense": "depreciation",
    "ifrs-full_PropertyPlantAndEquipment": "ppe",
    "ifrs-full_RetainedEarnings": "retained_earnings",
}

# Depreciation may appear in CF (cash flow) section with different account IDs.
# We collect these separately and use them as a fallback.
_CF_DEPRECIATION_IDS: dict[str, str] = {
    "ifrs-full_AdjustmentsForDepreciationExpense": "cf_depreciation",
    "ifrs-full_AdjustmentsForDepreciationAndAmortisationExpense": "cf_depreciation",
    "ifrs-full_AdjustmentsForAmortisationExpense": "cf_amortisation",
    "dart_DepreciationExpenseSellingGeneralAdministrativeExpenses": "is_depreciation_sga",
    "dart_AmortisationExpenseSellingGeneralAdministrativeExpenses": "is_amortisation_sga",
    "dart_TotalDepreciationExpense": "cf_depreciation",
    "dart_DepreciationExpenseCostOfSales": "is_depreciation_cogs",
}

ACCOUNT_NAME_MAP: dict[str, str] = {
    "매출액": "revenue",
    "수익(매출액)": "revenue",
    "영업수익": "revenue",
    "매출원가": "cogs",
    "영업비용": "cogs",
    "매출총이익": "gross_profit",
    "판매비와관리비": "sga",
    "영업이익": "operating_income",
    "영업이익(손실)": "operating_income",
    "이자비용": "interest_expense",
    "금융비용": "interest_expense",
    "당기순이익": "net_income",
    "당기순이익(손실)": "net_income",
    "자산총계": "total_assets",
    "현금및현금성자산": "cash_and_equivalents",
    "자본총계": "total_equity",
    "지배기업 소유주지분": "total_equity",
    "부채총계": "total_liabilities",
    "감가상각비": "depreciation",
    "유형자산": "ppe",
    "이익잉여금": "retained_earnings",
}

_IS_DIVS = {"IS", "CIS"}
_BS_DIVS = {"BS"}
_CF_DIVS = {"CF"}


@dataclass(frozen=True)
class ParsedFinancials:
    corp_code: str
    bsns_year: int
    reprt_code: str
    revenue: int | None
    cogs: int | None
    gross_profit: int | None
    sga: int | None
    operating_income: int | None
    interest_expense: int | None
    net_income: int | None
    total_assets: int | None
    cash_and_equivalents: int | None
    total_equity: int | None
    total_liabilities: int | None
    depreciation: int | None
    ppe: int | None
    retained_earnings: int | None


def _parse_amount(s: Any) -> int | None:
    if s is None:
        return None
    s = str(s).strip().replace(",", "").replace(" ", "")
    if not s or s == "-":
        return None
    try:
        return int(float(s))
    except (ValueError, OverflowError):
        return None


def _field_for_item(item: dict[str, Any]) -> str | None:
    account_id = (item.get("account_id") or "").strip()
    if account_id in ACCOUNT_ID_MAP:
        return ACCOUNT_ID_MAP[account_id]
    account_nm = (item.get("account_nm") or "").strip()
    return ACCOUNT_NAME_MAP.get(account_nm)


def parse_fnltt_single_acnt_all(
    response: dict[str, Any],
) -> ParsedFinancials | None:
    items = response.get("list") or []
    if not items:
        return None

    corp_code = (items[0].get("corp_code") or "").strip()
    try:
        bsns_year = int(items[0].get("bsns_year", 0))
    except (TypeError, ValueError):
        bsns_year = 0
    reprt_code = (items[0].get("reprt_code") or "").strip()

    extracted: dict[str, int | None] = {}
    cf_dep_parts: dict[str, int | None] = {}  # collect CF depreciation components

    for item in items:
        sj_div = (item.get("sj_div") or "").strip()
        account_id = (item.get("account_id") or "").strip()

        # Check for CF depreciation entries (fallback for IS depreciation)
        if account_id in _CF_DEPRECIATION_IDS:
            cf_key = _CF_DEPRECIATION_IDS[account_id]
            if cf_key not in cf_dep_parts:
                cf_dep_parts[cf_key] = _parse_amount(item.get("thstrm_amount"))

        field = _field_for_item(item)
        if not field or field in extracted:
            continue
        # BS 필드는 BS에서만, 손익 필드는 IS/CIS에서만
        if field in {"total_assets", "cash_and_equivalents", "total_equity",
                     "total_liabilities", "ppe", "retained_earnings"}:
            if sj_div and sj_div not in _BS_DIVS:
                continue
        elif field in {
            "revenue",
            "cogs",
            "gross_profit",
            "sga",
            "operating_income",
            "interest_expense",
            "net_income",
            "depreciation",
        }:
            if sj_div and sj_div not in _IS_DIVS:
                continue
        extracted[field] = _parse_amount(item.get("thstrm_amount"))

    if extracted.get("gross_profit") is None:
        rev = extracted.get("revenue")
        cogs = extracted.get("cogs")
        if rev is not None and cogs is not None:
            extracted["gross_profit"] = rev - cogs

    # Fallback: derive depreciation from CF section if not found in IS
    if extracted.get("depreciation") is None and cf_dep_parts:
        # Prefer cf_depreciation (total D&A from CF adjustments)
        if cf_dep_parts.get("cf_depreciation") is not None:
            extracted["depreciation"] = cf_dep_parts["cf_depreciation"]
            # Add amortisation if available
            if cf_dep_parts.get("cf_amortisation") is not None:
                extracted["depreciation"] += cf_dep_parts["cf_amortisation"]
        else:
            # Sum IS-level SGA/COGS depreciation components
            total = 0
            found = False
            for key in ("is_depreciation_sga", "is_depreciation_cogs",
                        "is_amortisation_sga"):
                val = cf_dep_parts.get(key)
                if val is not None:
                    total += val
                    found = True
            if found:
                extracted["depreciation"] = total

    return ParsedFinancials(
        corp_code=corp_code,
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        revenue=extracted.get("revenue"),
        cogs=extracted.get("cogs"),
        gross_profit=extracted.get("gross_profit"),
        sga=extracted.get("sga"),
        operating_income=extracted.get("operating_income"),
        interest_expense=extracted.get("interest_expense"),
        net_income=extracted.get("net_income"),
        total_assets=extracted.get("total_assets"),
        cash_and_equivalents=extracted.get("cash_and_equivalents"),
        total_equity=extracted.get("total_equity"),
        total_liabilities=extracted.get("total_liabilities"),
        depreciation=extracted.get("depreciation"),
        ppe=extracted.get("ppe"),
        retained_earnings=extracted.get("retained_earnings"),
    )
