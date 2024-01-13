"""Utility functions."""
import decimal
from decimal import Decimal
from typing import Any


def round_decimal(value: Decimal, digits: int = 0) -> Decimal:
    """Round decimal to given precision."""
    with decimal.localcontext() as ctx:
        ctx.rounding = decimal.ROUND_HALF_UP
        return Decimal(round(value, digits))


def strip_zeros(value: Decimal) -> str:
    """Strip trailing zeros from Decimal."""
    return f"{value:.10f}".rstrip("0").rstrip(".")


def decimal_from_str(price_str: str) -> Decimal:
    """Convert a number as string to a Decimal.

    Remove $ sign, and comma thousand separators so as to handle dollar amounts
    such as "$1,250.00".
    """
    return Decimal(price_str.replace("$", "").replace(",", ""))


def decimal_from_number_or_str(
    row: dict[str, Any],
    field_basename: str,
    field_float_suffix: str = "SortValue",
) -> Decimal:
    """Get a number from a row, preferably from the number field.

    Fall back to the string representation field, or default to Decimal(0)
    if the fields are not there or both have a value of None.
    """
    # We prefer native number to strings as more efficient/safer parsing
    float_name = f"{field_basename}{field_float_suffix}"
    if float_name in row and row[float_name] is not None:
        return Decimal(row[float_name])

    if field_basename in row and row[field_basename] is not None:
        return decimal_from_str(row[field_basename])

    return Decimal(0)
