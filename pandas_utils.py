import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
import re

# Precompiled UUID regex for performance (canonical 8-4-4-4-12 format)
UUID_REGEX = re.compile(
    r"^[a-fA-F0-9]{8}-"
    r"[a-fA-F0-9]{4}-"
    r"[1-5][a-fA-F0-9]{3}-"
    r"[89abAB][a-fA-F0-9]{3}-"
    r"[a-fA-F0-9]{12}$"
)

def is_uuid_column(series: pd.Series) -> bool:
    """
    Returns True if all non-null values in the Series look like
    valid UUIDs in canonical string form (8-4-4-4-12 hex).
    """
    s = series.dropna()

    # Empty after dropping NA → nothing to classify as UUID
    if s.empty:
        return False

    # Only string-like / object types can hold UUID strings
    if s.dtype.kind not in {"O", "U", "S"}:
        return False

    # Vectorized regex match → fast even for millions of rows
    return s.astype(str).str.match(UUID_REGEX).all()
