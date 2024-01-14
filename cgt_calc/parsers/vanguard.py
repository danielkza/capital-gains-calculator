"""Sharesight parser."""
from __future__ import annotations
from argparse import Action

import csv
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import logging
from pathlib import Path
import re
from typing import Any, Final, Iterable, Iterator, List

from cgt_calc.exceptions import ParsingError
from cgt_calc.model import ActionType, BrokerTransaction
from cgt_calc.util import RowIterator


logger=  logging.getLogger(__name__)

def parse_date(val: str) -> date:
    """Parse a Sharesight report date."""

    return datetime.strptime(val, "%d/%m/%Y").date()


def parse_decimal(val: str) -> Decimal:
    """Convert value to Decimal."""
    try:
        return Decimal(val.replace(",", ""))
    except InvalidOperation:
        raise ValueError(f"Bad decimal: {val}") from None


def maybe_decimal(row: dict[str, Any], key: str) -> Decimal | None:
    """Convert value to Decimal."""
    val = row.get(key, None)
    return parse_decimal(val) if val else None


class VanguardTransaction(BrokerTransaction):
    """Sharesight transaction.

    Just a marker type for now
    """


INVESTMENT_TRANSACTIONS_FIELDS = frozenset([
    "Date", "InvestmentName", "TransactionDetails", "Quantity", "Price", "Cost",
])

def parse_investment_transactions(rows: Iterator[list[str]]) -> Iterable[VanguardTransaction]:
    row = next(rows)
    # skip empty row
    if all(not f for f in row):
        row = next(rows)

    headers = list(row)
    if not INVESTMENT_TRANSACTIONS_FIELDS.issubset(headers):
        raise ValueError("Missing expected fields for Investment Transactions")

    for row in rows:
        if row[0] == 'Cost':
            break

        row_dict = dict(zip(headers, row))

        date = parse_date(row_dict["Date"])
        name = row_dict["InvestmentName"]
        details = row_dict["TransactionDetails"]
        amount = parse_decimal(row_dict["Cost"])

        if symbol_m := re.match(r"^.+ \([:alnum:]+\)$", name):
            symbol = symbol_m.group(1)
        else:
            symbol = name

        # The quantity provided in the column does not have as many decimal digits
        # as the description, and does not match the calculated amounts, so try
        # to parse from description

        if quantity_m := re.match(r"^(?:Bought|Sold) ([\d\.]+) ", details):
            quantity = parse_decimal(quantity_m.group(1))
        else:
            quantity = parse_decimal(row_dict["Quantity"])

        price = parse_decimal(row_dict["Price"])

        if re.match(r"^Sold ", details):
            tpe = ActionType.SELL
        elif re.match(r"^Bought ", details):
            tpe = ActionType.BUY
        else:
            logger.info(f"Ignoring unknown Vanguard transaction: {details}")
            continue

        # Make quantity always positive
        quantity = abs(quantity)
        # Make amount positive on sell and negative on buy
        amount = -amount

        yield VanguardTransaction(
            date=date,
            action=tpe,
            symbol=symbol,
            description=details,
            quantity=quantity,
            price=price,
            fees=Decimal(0),
            amount=amount,
            currency="GBP",
            broker="Vanguard",
        )


CASH_TRANSACTIONS_FIELDS = frozenset([
    "Date", "Details", "Amount", "Balance"
])

def parse_cash_transactions(rows: Iterator[list[str]]) -> Iterable[VanguardTransaction]:
    row = next(rows)
    # skip empty row
    if all(not f for f in row):
        row = next(rows)

    headers = list(row)
    if not CASH_TRANSACTIONS_FIELDS.issubset(headers):
        raise ValueError("Missing expected fields for Cash Transactions")

    print(headers)
    for row in rows:
        if row[0] == 'Balance':
            break

        print(row)
        row_dict = dict(zip(headers, row))
        print(row_dict)

        date = parse_date(row_dict["Date"])
        amount = parse_decimal(row_dict["Amount"])
        details = row_dict['Details']

        description = details
        if re.match(r"^Deposit", details):
            tpe = ActionType.TRANSFER
        elif match := re.match("^DIV: (.+)$", details):
            tpe = ActionType.DIVIDEND
            description = match.group(1)
        elif re.match("Cash Account Interest", details):
            tpe = ActionType.INTEREST
        elif re.match("^Account fee", details):
            tpe = ActionType.FEE
        else:
            logger.info(f"Ignoring unknown Vanguard transaction: {details}")
            continue

        yield VanguardTransaction(
            date=date,
            action=tpe,
            symbol=None,
            description=description,
            quantity=None,
            price=None,
            fees=Decimal(0),
            amount=amount,
            currency="GBP",
            broker="Vanguard",
        )

def read_vanguard_transactions(
    transactions_file: str,
) -> list[VanguardTransaction]:
    """Parse the Vanguard Transactions report."""

    with Path(transactions_file).open(encoding="utf-8") as csv_file:
        rows = list(csv.reader(csv_file))

    def gen():
        rows_iter = RowIterator(rows)
        for row in rows_iter:
            try:
                if row[0] == "Cash Transactions":
                    yield from parse_cash_transactions(rows_iter)
                elif row[0] == "Investment Transactions":
                    yield from parse_investment_transactions(rows_iter)
            except ValueError as err:
                raise ParsingError(f"{transactions_file}:{rows_iter.line}", str(err)) from None

    transactions = list(gen())
    transactions.sort(key=lambda t: t.date)
    return transactions
