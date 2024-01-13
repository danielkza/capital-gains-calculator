from decimal import Decimal

from cgt_calc import util

def test_decimal_from_str() -> None:
    """Test _decimal_from_str()."""
    assert util.decimal_from_str(  # pylint: disable=W0212
        "$123,456.23"
    ) == Decimal("123456.23")


def test_decimal_from_number_or_str_both() -> None:
    """Test _decimal_from_number_or_str() on float."""
    assert (
        util.decimal_from_number_or_str(  # pylint: disable=W0212
            {"key": "123.45", "keySortValue": Decimal("67.89")}, "key"
        )
        == Decimal("67.89")
    )


def test_decimal_from_number_or_str_float_null() -> None:
    """Test _decimal_from_number_or_str() on None float."""
    assert (
        util.decimal_from_number_or_str(  # pylint: disable=W0212
            {"key": "67.89", "keySortValue": None}, "key"
        )
        == Decimal("67.89")
    )


def test_decimal_from_number_or_str_float_custom_suffix() -> None:
    """Test _decimal_from_number_or_str_default_suffix() on float.

    With a custom suffix.
    """
    assert (
        util.decimal_from_number_or_str(  # pylint: disable=W0212
            {"keyMySuffix": Decimal("67.89")}, "key", "MySuffix"
        )
        == Decimal("67.89")
    )


def test_decimal_from_number_or_str_default() -> None:
    """Test _decimal_from_number_or_str() with absent keys."""
    assert (
        util.decimal_from_number_or_str(  # pylint: disable=W0212
            {"key": "123.45", "keySortValue": 67.89}, "otherkey"
        )
        == Decimal("0")
    )
