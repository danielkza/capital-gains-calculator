"""Charles Schwab parser."""

from __future__ import annotations

from collections import OrderedDict, defaultdict
import csv
import datetime
from decimal import Decimal
from enum import Enum
from itertools import chain
from pathlib import Path
from typing import Final

from cgt_calc.const import TICKER_RENAMES
from cgt_calc.exceptions import (
    ParsingError,
    SymbolMissingError,
    UnexpectedColumnCountError,
    UnexpectedRowCountError,
)
from cgt_calc.model import ActionType, BrokerTransaction
from cgt_calc.parsers.schwab_equity_award_json import read_schwab_equity_award_json_transactions
from cgt_calc.parsers.schwab_util import AwardPrices

OLD_COLUMNS_NUM: Final = 9
NEW_COLUMNS_NUM: Final = 8


class SchwabTransactionsFileRequiredHeaders(str, Enum):
    """Enum to list the headers in Schwab transactions file that we will use."""

    DATE = "Date"
    ACTION = "Action"
    SYMBOL = "Symbol"
    DESCRIPTION = "Description"
    PRICE = "Price"
    QUANTITY = "Quantity"
    FEES_AND_COMM = "Fees & Comm"
    AMOUNT = "Amount"


class AwardsTransactionsFileRequiredHeaders(str, Enum):
    """Enum to list the headers in Awards transactions file that we will use."""

    DATE = "Date"
    SYMBOL = "Symbol"
    FAIR_MARKET_VALUE_PRICE = "FairMarketValuePrice"


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
        "Visa Purchase",
        "MoneyLink Deposit",
        "MoneyLink Adj",  # likely a returned transfer
    ]:
        return ActionType.TRANSFER

    if label in "Stock Plan Activity":
        return ActionType.STOCK_ACTIVITY

    if label in [
        "Qualified Dividend",
        "Cash Dividend",
        "Qual Div Reinvest",
        "Div Adjustment",
        "Special Qual Div",
        "Non-Qualified Div",
    ]:
        return ActionType.DIVIDEND

    if label in ["NRA Tax Adj", "NRA Withholding", "Foreign Tax Paid"]:
        return ActionType.TAX

    if label == "ADR Mgmt Fee":
        return ActionType.FEE

    if label in ["Adjustment", "IRS Withhold Adj", "Wire Funds Adj"]:
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

    if label in ["Cash Merger", "Cash Merger Adj"]:
        return ActionType.CASH_MERGER

    raise ParsingError("schwab transactions", f"Unknown action: {label}")


class SchwabTransaction(BrokerTransaction):
    """Represent single Schwab transaction."""

    def __init__(
        self,
        row_dict: OrderedDict[str, str],
        file: str,
    ):
        """Create transaction from CSV row."""
        if len(row_dict) < NEW_COLUMNS_NUM or len(row_dict) > OLD_COLUMNS_NUM:
            # Old transactions had empty 9th column.
            raise UnexpectedColumnCountError(
                list(row_dict.values()), NEW_COLUMNS_NUM, file
            )
        if len(row_dict) == OLD_COLUMNS_NUM and list(row_dict.values())[-1] != "":
            raise ParsingError(file, f"Column {OLD_COLUMNS_NUM} should be empty")
        as_of_str = " as of "
        date_header = SchwabTransactionsFileRequiredHeaders.DATE.value
        if as_of_str in row_dict[date_header]:
            index = row_dict[date_header].find(as_of_str) + len(as_of_str)
            date_str = row_dict[date_header][index:]
        else:
            date_str = row_dict[date_header]
        try:
            date = datetime.datetime.strptime(date_str, "%m/%d/%Y").date()
        except ValueError as exc:
            raise ParsingError(
                file, f"Invalid date format: {date_str} from row: {row_dict}"
            ) from exc
        action_header = SchwabTransactionsFileRequiredHeaders.ACTION.value
        self.raw_action = row_dict[action_header]
        action = action_from_str(self.raw_action)
        symbol_header = SchwabTransactionsFileRequiredHeaders.SYMBOL.value
        symbol = row_dict[symbol_header] if row_dict[symbol_header] != "" else None
        if symbol is not None:
            symbol = TICKER_RENAMES.get(symbol, symbol)
        description_header = SchwabTransactionsFileRequiredHeaders.DESCRIPTION.value
        description = row_dict[description_header]
        price_header = SchwabTransactionsFileRequiredHeaders.PRICE.value
        price = (
            Decimal(row_dict[price_header].replace("$", ""))
            if row_dict[price_header] != ""
            else None
        )
        quantity_header = SchwabTransactionsFileRequiredHeaders.QUANTITY.value
        quantity = (
            Decimal(row_dict[quantity_header].replace(",", ""))
            if row_dict[quantity_header] != ""
            else None
        )
        fees_header = SchwabTransactionsFileRequiredHeaders.FEES_AND_COMM.value
        fees = (
            Decimal(row_dict[fees_header].replace("$", ""))
            if row_dict[fees_header] != ""
            else Decimal(0)
        )
        amount_header = SchwabTransactionsFileRequiredHeaders.AMOUNT.value
        amount = (
            Decimal(row_dict[amount_header].replace("$", ""))
            if row_dict[amount_header] != ""
            else None
        )

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
        row_dict: OrderedDict[str, str], file: str, awards_prices: AwardPrices
    ) -> SchwabTransaction:
        """Create and post process a SchwabTransaction."""
        transaction = SchwabTransaction(row_dict, file)
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


def _unify_schwab_cash_merger_trxs(
    transactions: list[SchwabTransaction],
) -> list[SchwabTransaction]:
    filtered: list[SchwabTransaction] = []
    for transaction in transactions:
        if transaction.raw_action == "Cash Merger Adj":
            assert (
                len(filtered) > 0
            ), "Cash Merger Adj must be precedeed by a Cash Merger transaction"
            assert filtered[-1].raw_action == "Cash Merger"
            assert filtered[-1].description == transaction.description
            assert filtered[-1].symbol == transaction.symbol
            assert filtered[-1].date == transaction.date
            assert filtered[-1].quantity is None
            assert filtered[-1].price is None
            assert filtered[-1].amount is not None
            assert transaction.amount is None
            assert transaction.quantity is not None
            # the quantity is negative but
            # because we store it as a 'sell' we need it positive
            filtered[-1].quantity = -1 * transaction.quantity
            filtered[-1].price = filtered[-1].amount / filtered[-1].quantity
            filtered[-1].fees += transaction.fees
            print(
                "WARNING: Cash Merger support is not complete and doesn't cover the "
                "cases when shares are received aside from cash,  "
                "please review this transaction carefully: "
                f"{filtered[-1]}"
            )
        else:
            filtered.append(transaction)
    return filtered


def read_schwab_transactions(
    transactions_file: str | None,
    transactions_folder: str | None,
    schwab_award_transactions_file: str | None,
    schwab_award_transactions_folder: str | None,
    award_prices: AwardPrices = AwardPrices({}),
) -> list[BrokerTransaction]:
    """Read Schwab transactions from file."""
    
    awards_prices = award_prices.merge(_read_schwab_awards_all(schwab_award_transactions_file, schwab_award_transactions_folder))

    files: list[str] = []
    if transactions_file:
        files.append(transactions_file)
        
    if transactions_folder:
        files.extend(Path(transactions_folder).glob("*.csv"))

    all_transactions: list[SchwabTransaction] = []
    
    for file in files:
        print(f"Parsing {file}")
        try:
            with Path(file).open(encoding="utf-8") as csv_file:
                lines = list(csv.reader(csv_file))
                headers = lines[0]

                required_headers = set(
                    {header.value for header in SchwabTransactionsFileRequiredHeaders}
                )
                if not required_headers.issubset(headers):
                    raise ParsingError(
                        transactions_file,
                        "Missing columns in Schwab transaction file: "
                        f"{required_headers.difference(headers)}",
                    )

                # Remove header
                lines = lines[1:]
                transactions = [
                    SchwabTransaction.create(
                        OrderedDict(zip(headers, row)), transactions_file, awards_prices
                    )
                    for row in lines
                    if any(row)
                ]
                transactions = _unify_schwab_cash_merger_trxs(transactions)
                transactions.reverse()
                all_transactions.extend(transactions)
        except FileNotFoundError:
            print(f"WARNING: Couldn't locate Schwab transactions file({file})")

    all_transactions.sort(key=lambda k: k.date)
    return all_transactions


def _read_schwab_awards_all(
    schwab_award_transactions_file: str | None,
    schwab_award_transactions_folder: str | None,
) -> AwardPrices:
    files: list[str] = []
    if schwab_award_transactions_file:
        files.append(schwab_award_transactions_file)
        
    if schwab_award_transactions_folder:
        files.extend(Path(schwab_award_transactions_folder).glob("*.csv"))
        
    award_prices = AwardPrices(award_prices={})
    for file in files:
        award_prices = award_prices.merge(_read_schwab_awards(file))
        
    return award_prices


def _read_schwab_awards(
    schwab_award_transactions_file: str | None,
) -> AwardPrices:
    """Read initial stock prices from CSV file."""
    initial_prices: dict[datetime.date, dict[str, Decimal]] = defaultdict(dict)

    headers = []

    lines = []
    if schwab_award_transactions_file is not None:
        try:
            with Path(schwab_award_transactions_file).open(
                encoding="utf-8"
            ) as csv_file:
                lines = list(csv.reader(csv_file))
                headers = lines[0]
                required_headers = set(
                    {header.value for header in AwardsTransactionsFileRequiredHeaders}
                )
                if not required_headers.issubset(headers):
                    raise ParsingError(
                        schwab_award_transactions_file,
                        "Missing columns in awards file: "
                        f"{required_headers.difference(headers)}",
                    )

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

    for upper_row, lower_row in zip(lines[::2], lines[1::2]):
        # in this format each row is split into two rows,
        # so we combine them safely below
        row = []
        for upper_col, lower_col in zip(upper_row, lower_row):
            assert upper_col == "" or lower_col == ""
            row.append(upper_col + lower_col)

        if len(row) != len(headers):
            raise UnexpectedColumnCountError(
                row, len(headers), schwab_award_transactions_file or ""
            )

        row_dict = OrderedDict(zip(headers, row))
        date_header = AwardsTransactionsFileRequiredHeaders.DATE.value
        date_str = row_dict[date_header]
        try:
            date = datetime.datetime.strptime(date_str, "%Y/%m/%d").date()
        except ValueError:
            date = datetime.datetime.strptime(date_str, "%m/%d/%Y").date()
        symbol_header = AwardsTransactionsFileRequiredHeaders.SYMBOL.value
        symbol = row_dict[symbol_header] if row_dict[symbol_header] != "" else None
        fair_market_value_price_header = (
            AwardsTransactionsFileRequiredHeaders.FAIR_MARKET_VALUE_PRICE.value
        )
        price = (
            Decimal(row_dict[fair_market_value_price_header].replace("$", ""))
            if row_dict[fair_market_value_price_header] != ""
            else None
        )
        if symbol is not None and price is not None:
            symbol = TICKER_RENAMES.get(symbol, symbol)
            initial_prices[date][symbol] = price
    return AwardPrices(award_prices=dict(initial_prices))

def read_schwab_combined_transactions(
    schwab_transactions_file: str | None,
    schwab_transactions_folder: str | None,
    schwab_awards_transactions_file: str | None,
    schwab_awards_transactions_folder: str | None,
    schwab_equity_award_json_transactions_file: str | None,
    schwab_equity_award_json_transactions_folder: str | None,
) -> list[BrokerTransaction]:    
    equity_transactions: list[SchwabTransaction] = []
    transactions: list[SchwabTransaction] = []

    award_prices = AwardPrices({})
    equity_awards_sym_dates: set[tuple[datetime.date, str]] = set()

    if schwab_equity_award_json_transactions_file is not None or schwab_equity_award_json_transactions_folder is not None:
        equity_transactions, award_prices = read_schwab_equity_award_json_transactions(
            transactions_file=schwab_equity_award_json_transactions_file,
            transactions_folder=schwab_equity_award_json_transactions_folder,
        )
        equity_awards_sym_dates = set((tr.date, tr.symbol) for tr in equity_transactions)
    else:
        print("INFO: No schwab Equity Award JSON file provided")
        
    if schwab_transactions_file is not None or schwab_transactions_folder is not None:
        transactions = read_schwab_transactions(
            transactions_file=schwab_transactions_file,
            schwab_award_transactions_file=schwab_awards_transactions_file,
            transactions_folder=schwab_transactions_folder,
            schwab_award_transactions_folder=schwab_awards_transactions_folder,
            award_prices=award_prices,
        )
    else:
        print("INFO: No schwab file provided")
    
    if equity_transactions:
        # If we have been provided Equity Awards data, we should ignore STOCK_ACTIVITY transactions
        # from dates present in both. The Equity Awards data is more accurate.
        
        clean_transactions: list[SchwabTransaction] = []
        for tr in transactions:
            if (tr.date, tr.symbol) in equity_awards_sym_dates:
                print(f"INFO: Removing STOCK_ACTIVITY transaction already present in Equity Awards data: {tr.symbol}:{tr.date}:{tr.quantity}")
            else:
                clean_transactions.append(tr)
        
        transactions = sorted(chain(equity_transactions, clean_transactions),  key=lambda k: k.date)
    
    return transactions