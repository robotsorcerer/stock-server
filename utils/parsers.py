def parse_dollar(s):
    """Parse '$123.45' or 'N/A' to float, or return None."""
    if not s or str(s).strip() in ("N/A", "NA", "--", ""):
        return None
    try:
        return float(str(s).replace("$", "").replace(",", "").strip())
    except ValueError:
        return None


def parse_volume(s):
    """Parse '1,234,567' or '45,565,353.988652' to int."""
    if not s or str(s).strip() in ("N/A", "NA", "--", ""):
        return 0
    try:
        return int(float(str(s).replace(",", "").strip()))
    except ValueError:
        return 0
