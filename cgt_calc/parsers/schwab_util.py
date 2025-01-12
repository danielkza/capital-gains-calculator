
from dataclasses import dataclass, replace
import datetime
from decimal import Decimal

from cgt_calc.const import TICKER_RENAMES


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
    
    def merge(self, other: 'AwardPrices') -> None:
        """Merge two AwardPrices objects."""
        
        new_prices = self.award_prices.copy()
        new_prices.update(other.award_prices)
        return replace(self, award_prices=new_prices)

