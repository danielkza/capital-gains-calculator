"""Unit tests on schwab_equity_award_json.py."""
from __future__ import annotations

import datetime
from decimal import Decimal

from cgt_calc.model import ActionType
from cgt_calc.parsers import schwab_equity_award_json


def test_schwab_transaction() -> None:
    """Test read_schwab_equity_award_json_transactions()."""
    transactions = schwab_equity_award_json.read_schwab_equity_award_json_transactions(
        "tests/test_data/schwab_equity_award.json"
    )

    assert transactions[0].date == datetime.date(2022, 4, 25)
    assert transactions[0].action == ActionType.STOCK_ACTIVITY
    assert transactions[0].symbol == "GOOG"
    assert transactions[0].quantity == Decimal("67.2")
    assert transactions[0].price == Decimal("125.6445")
    assert transactions[0].fees == Decimal("0")
    assert transactions[0].currency == "USD"
    assert transactions[0].broker == "Charles Schwab"

    assert transactions[1].date == datetime.date(2022, 6, 10)
    assert transactions[1].action == ActionType.SELL
    assert transactions[1].quantity == Decimal("62.601495")
    assert transactions[1].price == Decimal("113.75")
    assert transactions[1].fees == Decimal("0.17")

    assert transactions[2].date == datetime.date(2022, 10, 25)
    assert transactions[2].action == ActionType.STOCK_ACTIVITY
    assert transactions[2].quantity == Decimal("10.45")
    assert transactions[2].price == Decimal("112.42")
    assert transactions[2].fees == Decimal("0")

    assert transactions[3].date == datetime.date(2022, 11, 14)
    assert transactions[3].action == ActionType.SELL
    assert transactions[3].quantity == Decimal("12.549")
    assert transactions[3].price.quantize(  # type: ignore
        Decimal(".000001")
    ).normalize() == Decimal("2051.597737")
    assert transactions[3].amount == Decimal("25745")
    assert transactions[3].fees == Decimal("0.50")
