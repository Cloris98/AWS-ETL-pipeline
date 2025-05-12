import re


def fix_date_form(value: str):
    """
    exchange the date format if it does not match yyyy-mm-dd, return it original if it matches.
    """

    if value is None:
        return "0000-00-00"

    match = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", value)
    if not match:
        return "0000-00-00"
    else:
        year, month, day = match.groups()
        y, m, d = int(year), int(month), int(day)
        if m > 12 or d > 31 or d < 1:
            return "0000-00-00"
    return value


