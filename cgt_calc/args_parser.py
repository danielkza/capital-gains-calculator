"""Parse command line arguments."""

import argparse
import datetime

from .const import (
    DEFAULT_EXCHANGE_RATES_FILE,
    DEFAULT_REPORT_PATH,
    DEFAULT_SPIN_OFF_FILE,
)


def get_last_elapsed_tax_year() -> int:
    """Get last ended tax year."""
    now = datetime.datetime.now()
    if now.date() >= datetime.date(now.year, 4, 6):
        return now.year - 1
    return now.year - 2


def create_parser() -> argparse.ArgumentParser:
    """Create ArgumentParser."""
    parser = argparse.ArgumentParser(
        description="Calculate capital gains from stock transactions.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=get_last_elapsed_tax_year(),
        nargs="?",
        help="First year of the tax year to calculate gains on (default: %(default)d)",
    )
    parser.add_argument(
        "--raw",
        type=str,
        nargs="?",
        help="file containing the exported transactions in a raw format csv format",
    )
    parser.add_argument(
        "--schwab",
        type=str,
        nargs="?",
        help="file containing the exported transactions from Charles Schwab",
    )
    parser.add_argument(
        "--schwab-folder",
        type=str,
        nargs="?",
        help="folder containing the exported transactions from Charles Schwab",
    )
    parser.add_argument(
        "--schwab-award",
        type=str,
        default=None,
        nargs="?",
        help="file containing schwab award data for stock prices",
    )
    parser.add_argument(
        "--schwab-award-folder",
        type=str,
        default=None,
        nargs="?",
        help="folder containing schwab award data for stock prices",
    )
    parser.add_argument(
        "--schwab_equity_award_json",
        "--schwab-equity-award-json",
        dest="schwab_equity_award_json",
        type=str,
        default=None,
        nargs="?",
        help="file containing schwab equity award transactions data in JSON format",
    )
    parser.add_argument(
        "--schwab_equity_award_json-folder",
        "--schwab-equity-award-json-folder",
        dest="schwab_equity_award_json_folder",
        type=str,
        default=None,
        nargs="?",
        help="file containing schwab equity award transactions data in JSON format",
    )
    parser.add_argument(
        "--trading212",
        type=str,
        nargs="?",
        help="folder containing the exported transaction files from Trading 212",
    )
    parser.add_argument(
        "--mssb",
        type=str,
        nargs="?",
        help="folder containing the exported transaction files from Morgan Stanley",
    )
    parser.add_argument(
        "--sharesight",
        type=str,
        nargs="?",
        help="folder containing reports from Sharesight in CSV format",
    )

    parser.add_argument(
        "--exchange-rates-file",
        type=str,
        default=DEFAULT_EXCHANGE_RATES_FILE,
        nargs="?",
        help="output file for monthly exchange rates from HMRC",
    )
    parser.add_argument(
        "--spin-offs-file",
        type=str,
        default=DEFAULT_SPIN_OFF_FILE,
        nargs="?",
        help="output file for spin offs data",
    )
    parser.add_argument(
        "--initial-prices",
        type=str,
        default=None,
        nargs="?",
        help="file containing stock prices in USD at the moment of vesting, split, etc",
    )
    parser.add_argument(
        "--report",
        type=str,
        default=DEFAULT_REPORT_PATH,
        nargs="?",
        help="where to save the generated PDF report (default: %(default)s)",
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help="do not generate PDF report",
    )
    parser.add_argument(
        "--no-balance-check",
        dest="balance_check",
        action="store_false",
        default=True,
    )
    parser.add_argument(
        "--unrealized-gains",
        dest="calc_unrealized_gains",
        action="store_true",
        default=False,
        help=(
            "show an estimation of the gains/loss you would incur"
            " if you were to sell your holdings, under the standard 104 rule."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="enable extra logging",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="print version",
    )
    # For testing only
    parser.add_argument(
        "--no-pdflatex",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    return parser
