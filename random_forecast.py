import numpy as np
import pandas as pd
import quandl
import datetime
quandl.ApiConfig.api_key = "Y9_EnCeHAc1ZWrxc3U2J"

from Backtest import Strategy, Portfolio

class RandomForecastingStrategy(Strategy):
    """Derives from Strategy to produce a set of signals that
        are randomly generated long/shorts. Clearly a nonsensical
        strategy, but perfectly acceptable for demonstrating the
        backtesting infrastructure!"""

    def __init__(self, symbol, bars):
        "Requires the symbol ticker and the pandas DataFrame of bars"
        self.symbol = symbol
        self.bars = bars

    def generate_signals(self):
        "Creates a pandas DataFrame of random signals."
        signals = pd.DataFrame(index=self.bars.index)
        signals['signal'] = np.sign(np.random.randn(len(signals)))

        # The first five elements are set to zero in order to minimise
        # upstream NaN errors in the forecaster.
        signals['signal'][0:5] = 0.0

        return signals

class MarketOnOpenPortfolio(Portfolio):
    """Inherits Portfolio to create a system that purchases 100 units of
        a particular symbol upon a long/short signal, assuming the market
        open price of a bar.

        In addition, there are zero transaction costs and cash can be immediately
        borrowed for shorting (no margin posting or interest requirements).

        Requires:
        symbol - A stock symbol which forms the basis of the portfolio.
        bars - A DataFrame of bars for a symbol set.
        signals - A pandas DataFrame of signals (1, 0, -1) for each symbol.
        initial_capital - The amount in cash at the start of the portfolio."""

    def __init__(self, symbol, bars, signals, initial_capital = 10000.0):
        self.symbol = symbol
        self.bars = bars
        self.signals = signals
        self.initial_capital = float(initial_capital)
        self.positions = self.generate_positions()

    def generate_positions(self):
        """Creates a 'positions' DataFrame that simply longs or shorts
            100 of the particular symbol based on the forecast signals of
            {1, 0, -1} from the signals DataFrame."""
        positions = pd.DataFrame(index = signals.index).fillna(0.0)
        positions[self.symbol] = 100 * signals['signal']

        return positions

    def backtest_portfolio(self):
        """Constructs a portfolio from the positions DataFrame by
            assuming the ability to trade at the precise market open price
            of each bar (an unrealistic assumption!).

            Calculates the total of cash and the holdings (market price of
            each position per bar), in order to generate an equity curve
            ('total') and a set of bar-based returns ('returns').

            Returns the portfolio object to be used elsewhere."""

        # Construct the portfolio DataFrame to use the same index
        # as 'positions' and with a set of 'trading orders' in the
        # 'pos_diff' object, assuming market open prices.

        portfolio = pd.DataFrame(data = self.positions[symbol] * self.bars['Open'])
        portfolio.columns = [symbol]
        pos_diff = pd.DataFrame(data = self.positions.diff())

        # Create the 'holdings' and 'cash' series by running through
        # the trades and adding/subtracting the relevant quantity from
        # each column
        portfolio['holdings'] = (self.positions[symbol] * self.bars['Open']).sum()
        portfolio['cash'] = self.initial_capital - (pos_diff[symbol] * self.bars['Open']).cumsum()

        # Finalise the total and bar-based returns based on the 'cash'
        # and 'holdings' figures for the portfolio
        portfolio['total'] = portfolio['cash'] + portfolio['holdings']
        portfolio['returns'] = portfolio['total'].pct_change().cumsum()
        return portfolio

if __name__ == "__main__":
    # Obtain daily bars of SPY from Quandl
    start_date = datetime.datetime(2000, 1, 1)
    end_date = datetime.date.today()
    symbol = "GOOG"
    bars = quandl.get('WIKI/' + symbol, start_date = start_date, end_date = end_date, paginate = True)

    # Create a set of random forecasting signals for SPY
    rfs = RandomForecastingStrategy(symbol, bars)
    signals = rfs.generate_signals()

    # Create a portfolio of SPY
    portfolio = MarketOnOpenPortfolio(symbol, bars, signals, initial_capital = 10000.0)
    returns = portfolio.backtest_portfolio()

    print(returns.tail(10))
