### Herculus
import pandas as pd
import re

def parse_coupon(text: str):
    """
    Parse coupon with proper extraction order and boundary checking.
    """
    import re
    text = str(text).strip().upper()
    if not text or text in ["NAN", ""]:
        return ["--"] * 10
    
    # Initialize
    ref_rate = total_spread = pik_spread = floor = ceiling = etp = None
    cash_spread = cash_int = pik_int = None
    fixed_float = "Floating"
    
    # --- 1️⃣ Extract Reference Rate (with better word boundaries) ---
    ref_match = re.search(
        r"\b((?:\d+-MONTH\s+)?SOFR|EURIBOR|LIBOR|PRIME|DAILY\s+SONIA|BASE\s+RATE)\b",
        text
    )
    if ref_match:
        ref_rate = ref_match.group(1).strip().title()
    
    # --- 2️⃣ Extract PIK Interest FIRST (before cash interest) ---
    pik_int_match = re.search(r"PIK\s+INTEREST\s+([\d\.]+)%", text)
    if pik_int_match:
        pik_int = pik_int_match.group(1)
    
    # --- 3️⃣ Extract PIK Spread (distinct from PIK Interest) ---
    pik_spread_match = re.search(r"PIK\s+(?!INTEREST)([\d\.]+)%", text)
    if pik_spread_match:
        pik_spread = pik_spread_match.group(1)
    
    # --- 4️⃣ Extract Total Spread (must be after reference rate) ---
    if ref_rate:
        # Look for spread immediately after reference rate
        spread_match = re.search(
            rf"{re.escape(ref_rate.upper())}\s*[\+\-]\s*([\d\.]+)%",
            text
        )
        if spread_match:
            total_spread = spread_match.group(1)
    
    # --- 5️⃣ Extract Floor (with multiple pattern support) ---
    floor_match = re.search(r"FLOOR\s+(?:RATE\s+)?([\d\.]+)%", text)
    if floor_match:
        floor = floor_match.group(1)
    
    # --- 6️⃣ Extract Ceiling ---
    ceiling_match = re.search(r"(?:CEILING|CAP)\s+(?:RATE\s+)?([\d\.]+)%", text)
    if ceiling_match:
        ceiling = ceiling_match.group(1)
    
    # --- 7️⃣ Extract Exit Fee/ETP ---
    etp_match = re.search(r"([\d\.]+)%\s+EXIT\s+FEE", text)
    if etp_match:
        etp = etp_match.group(1)
    
    # --- 8️⃣ Extract Cash Spread ---
    cash_spread_match = re.search(r"CASH\s+(?:SPREAD\s+)?[\+\-]?\s*([\d\.]+)%", text)
    if cash_spread_match:
        cash_spread = cash_spread_match.group(1)
    
    # --- 9️⃣ Extract Cash Interest (but NOT if it's PIK Interest) ---
    cash_int_match = re.search(
        r"(?:CASH\s+INTEREST|(?<!PIK\s)FIXED)\s+([\d\.]+)%",
        text
    )
    if cash_int_match:
        cash_int = cash_int_match.group(1)
    
    # --- 🔟 Determine Fixed/Floating ---
    if ref_rate and ref_rate != "--":
        fixed_float = "Floating"
    else:
        if "FIXED" in text:
            fixed_float = "Fixed"
        else:
            # Has interest but no reference rate = Fixed
            has_interest = cash_int or pik_int
            has_ref_spread = total_spread is not None
            if has_interest and not has_ref_spread:
                fixed_float = "Fixed"
    
    # --- Final Assembly ---
    result = [
        ref_rate, total_spread, pik_spread, floor, ceiling,
        etp, cash_spread, cash_int, pik_int, fixed_float
    ]
    return [v if v not in [None, "", "NAN"] else "--" for v in result]

def bifurcate_coupon_columns(df: pd.DataFrame, source_col: str) -> pd.DataFrame:
    columns = [
        "Reference Rate", "Total Spread", "PIK Spread", "Floor", "Ceiling",
        "ETP", "Cash Spread", "Cash Interest", "PIK Interest", "Fixed/Floating"
    ]
    
    if source_col not in df.columns:
        raise KeyError(f"Column '{source_col}' not found in DataFrame")
    
    parsed = df[source_col].apply(parse_coupon)
    parsed_df = pd.DataFrame(parsed.tolist(), columns=columns, index=df.index)  # ✅ PRESERVE INDEX
    return pd.concat([df, parsed_df], axis=1)