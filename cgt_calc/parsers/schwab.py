"""Charles Schwab parser."""
from __future__ import annotations

from collections import defaultdict
import csv
from dataclasses import dataclass
import datetime
from decimal import Decimal
import itertools
import json
from pathlib import Path
from typing import Any, Sequence

from cgt_calc.const import TICKER_RENAMES
from cgt_calc.exceptions import (
    ParsingError,
    SymbolMissingError,
    UnexpectedColumnCountError,
    UnexpectedRowCountError,
)
from cgt_calc.model import ActionType, BrokerTransaction
from cgt_calc.util import decimal_from_number_or_str


@dataclass
class AwardPrices:
    """Class to store initial stock prices."""

    award_prices: dict[datetime.date, dict[str, Decimal]]

    def get(self, date: datetime.date, symbol: str) -> tuple[datetime.date, Decimal]:
        """Get initial stock price at given date."""
        # Award dates may go back for few days, depending on
        # holidays or weekends, so we do a linear search
        # in the past to find the award price
        symbol = TICKER_RENAMES.get(symbol, symbol)
        for i in range(7):
            to_search = date - datetime.timedelta(days=i)

            if (
                to_search in self.award_prices
                and symbol in self.award_prices[to_search]
            ):
                return (to_search, self.award_prices[to_search][symbol])
        raise KeyError(f"Award price is not found for symbol {symbol} for date {date}")


def action_from_str(label: str) -> ActionType:
    """Convert string label to ActionType."""
    if label == "Buy":
        return ActionType.BUY

    if label == "Sell":
        return ActionType.SELL

    if label in [
        "MoneyLink Transfer",
        "Misc Cash Entry",
        "Service Fee",
        "Wire Funds",
        "Wire Sent",
        "Funds Received",
        "Journal",
        "Cash In Lieu",
    ]:
        return ActionType.TRANSFER

    if label == "Stock Plan Activity":
        return ActionType.STOCK_ACTIVITY

    if label in ["Qualified Dividend", "Cash Dividend"]:
        return ActionType.DIVIDEND

    if label in ["NRA Tax Adj", "NRA Withholding", "Foreign Tax Paid"]:
        return ActionType.TAX

    if label == "ADR Mgmt Fee":
        return ActionType.FEE

    if label in ["Adjustment", "IRS Withhold Adj"]:
        return ActionType.ADJUSTMENT

    if label in ["Short Term Cap Gain", "Long Term Cap Gain"]:
        return ActionType.CAPITAL_GAIN

    if label == "Spin-off":
        return ActionType.SPIN_OFF

    if label == "Credit Interest":
        return ActionType.INTEREST

    if label == "Reinvest Shares":
        return ActionType.REINVEST_SHARES

    if label == "Reinvest Dividend":
        return ActionType.REINVEST_DIVIDENDS

    if label == "Wire Funds Received":
        return ActionType.WIRE_FUNDS_RECEIVED

    if label == "Stock Split":
        return ActionType.STOCK_SPLIT

    raise ParsingError("schwab transactions", f"Unknown action: {label}")


class SchwabTransaction(BrokerTransaction):
    """Represent single Schwab transaction."""

    def __init__(
        self,
        row: dict[str, Any],
        file: str,
    ):
        """Create transaction from CSV row."""
        if not FIELDS.issubset(row.keys()):
            raise ParsingError(file, "Row is missing expected fields")

        as_of_str = " as of "
        if as_of_str in row["Date"]:
            index = row["Date"].find(as_of_str) + len(as_of_str)
            date_str = row["Date"][index:]
        else:
            date_str = row["Date"]
        date = datetime.datetime.strptime(date_str, "%m/%d/%Y").date()
        self.raw_action = row["Action"]
        action = action_from_str(self.raw_action)
        symbol = row["Symbol"] if row["Symbol"] != "" else None
        if symbol is not None:
            symbol = TICKER_RENAMES.get(symbol, symbol)
        description = row["Description"]
        quantity = decimal_from_number_or_str(row, "Quantity") if row["Quantity"] != "" else None
        price = decimal_from_number_or_str(row, "Price") if row["Price"] != "" else None
        fees = decimal_from_number_or_str(row, "Fees & Comm") if row["Fees & Comm"] != "" else Decimal(0)
        amount = decimal_from_number_or_str(row, "Amount") if row["Amount"] != "" else None

        currency = "USD"
        broker = "Charles Schwab"
        super().__init__(
            date,
            action,
            symbol,
            description,
            quantity,
            price,
            fees,
            amount,
            currency,
            broker,
        )

    @staticmethod
    def create(
        row: dict[str, Any], file: str, awards_prices: AwardPrices
    ) -> SchwabTransaction:
        """Create and post process a SchwabTransaction."""
        transaction = SchwabTransaction(row, file)
        if (
            transaction.price is None
            and transaction.action == ActionType.STOCK_ACTIVITY
        ):
            symbol = transaction.symbol
            if symbol is None:
                raise SymbolMissingError(transaction)
            # Schwab transaction list contains sometimes incorrect date
            # for awards which don't match the PDF statements.
            # We want to make sure to match date and price form the awards
            # spreadsheet.
            transaction.date, transaction.price = awards_prices.get(
                transaction.date, symbol
            )
        return transaction


HEADERS = [
    "Date",
    "Action",
    "Symbol",
    "Description",
    "Quantity",
    "Price",
    "Fees & Comm",
    "Amount",
]
FIELDS = frozenset(HEADERS)


def read_schwab_transactions_csv(
        transactions_file: str, awards_prices: AwardPrices,
        ignore_stock_activity: bool = False
) -> Sequence[SchwabTransaction]:
    with Path(transactions_file).open(encoding="utf-8") as csv_file:
        lines = list(csv.reader(csv_file))

        if lines[0] != HEADERS:
            raise ParsingError(
                transactions_file,
                "First line of Schwab transactions file must be something like "
                "'Transactions for account ...'",
            )

        if len(lines[1]) < 8 or len(lines[1]) > 9:
            raise ParsingError(
                transactions_file,
                "Second line of Schwab transactions file must be a header"
                " with 8 columns",
            )

        # Remove header
        lines = lines[1:]
        rows: list[dict[str, str]] = []
        for line in lines:
            if len(line) < 8 or len(line) > 9:
                # Old transactions had empty 9th column.
                raise UnexpectedColumnCountError(line, 8, transactions_file)

            if len(line) == 9 and line[8] != "":
                raise ParsingError(transactions_file, "Column 9 should be empty")

            if line[0] == "Transactions Total":
                continue

            row = dict(zip(HEADERS, line))
            rows.append(row)

        transactions = [
            SchwabTransaction.create(row, transactions_file, awards_prices)
            for row in rows
            if not ignore_stock_activity or row["Action"] != "Stock Plan Activity"
        ]
        transactions.reverse()
        return list(transactions)


def read_schwab_transactions_json(
        transactions_file: str, awards_prices: AwardPrices,
        ignore_stock_activity: bool = False,
) -> Sequence[SchwabTransaction]:
    with Path(transactions_file).open(encoding="utf-8") as json_file:
        data = json.load(
            json_file, parse_float=Decimal, parse_int=Decimal)

        rows = data['BrokerageTransactions']
        for i, row in enumerate(rows, 1):
            if not FIELDS.issubset(row.keys()):
                raise ParsingError(
                    transactions_file, f"Transaction #{i} does not have expected fields")

        transactions = [
            SchwabTransaction.create(row, transactions_file, awards_prices)
            for row in rows
            if not ignore_stock_activity or row["Action"] != "Stock Plan Activity"
        ]
        transactions.reverse()
        return list(transactions)


def read_schwab_transactions(
    transactions_file: str, schwab_award_transactions_file: str | None,
    ignore_stock_activity: bool = False
) -> Sequence[BrokerTransaction]:
    """Read Schwab transactions from file."""
    awards_prices = _read_schwab_awards(schwab_award_transactions_file)
    try:
        if transactions_file.endswith(".json"):
            transactions = read_schwab_transactions_json(
                transactions_file, awards_prices,
                ignore_stock_activity=ignore_stock_activity)
        else:
            transactions = read_schwab_transactions_csv(
                transactions_file, awards_prices,
                ignore_stock_activity=ignore_stock_activity)

        return transactions
    except FileNotFoundError:
        print(f"WARNING: Couldn't locate Schwab transactions file({transactions_file})")
        return []


def _read_schwab_awards(
    schwab_award_transactions_file: str | None,
) -> AwardPrices:
    """Read initial stock prices from CSV file."""
    initial_prices: dict[datetime.date, dict[str, Decimal]] = defaultdict(dict)

    lines = []
    if schwab_award_transactions_file is not None:
        try:
            with Path(schwab_award_transactions_file).open(
                encoding="utf-8"
            ) as csv_file:
                lines = list(csv.reader(csv_file))
                # Remove headers
                lines = lines[1:]
        except FileNotFoundError:
            print(
                "WARNING: Couldn't locate Schwab award "
                f"file({schwab_award_transactions_file})"
            )
    else:
        print("WARNING: No schwab award file provided")

    modulo = len(lines) % 2
    if modulo != 0:
        raise UnexpectedRowCountError(
            len(lines) - modulo + 2, schwab_award_transactions_file or ""
        )

    for row in zip(lines[::2], lines[1::2]):
        if len(row) != 2:
            raise UnexpectedColumnCountError(
                list(itertools.chain(*row)), 2, schwab_award_transactions_file or ""
            )

        lapse_main, lapse_data = row

        if len(lapse_main) != 15:
            raise UnexpectedColumnCountError(
                lapse_main, 8, schwab_award_transactions_file or ""
            )
        if len(lapse_data) != 15:
            raise UnexpectedColumnCountError(
                lapse_data, 8, schwab_award_transactions_file or ""
            )

        date_str = lapse_main[0]
        try:
            date = datetime.datetime.strptime(date_str, "%Y/%m/%d").date()
        except ValueError:
            date = datetime.datetime.strptime(date_str, "%m/%d/%Y").date()
        symbol = lapse_main[2] if lapse_main[2] != "" else None
        price = Decimal(lapse_data[10].replace("$", "")) if lapse_data[10] != "" else None
        if symbol is not None and price is not None:
            symbol = TICKER_RENAMES.get(symbol, symbol)
            initial_prices[date][symbol] = price
    return AwardPrices(award_prices=dict(initial_prices))
