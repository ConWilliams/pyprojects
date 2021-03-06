import datetime
import numpy as np
import pandas as pd
import Queue

from abc import ABCMeta, abstractmethod
from math import floor

from event import FillEvent, OrderEvent
from Performance import create_sharpe_ratio, create_drawdowns

class Portfolio(object):
    """ Handles the positions and market value of all instruments
        at a resolution of a 'bar', in this case EOD. """

    __metaclass__ = ABCMeta

    @abstractmethod
    def update_signal(self, event):
        """ Acts on a SignalEvent to generate a new order based on
            portfolio logic. """

        raise NotImplementedError("Should implement update_signal()")

    @abstractmethod
    def update_fill(self, event):
        """ Updates the portfolio current positions and holdings from a FillEvent."""

        raise NotImplementedError("Should implement update_fill()")

class NaivePortfolio(Portfolio):
    """ The NaivePortfolio object is designed to send orders to a brokerage object
        with a constant quantity size blindly, that is without any risk management or
        position sizing.  BuyAndHoldStrategy. """

    def __init__(self, bars, events, start_date, initial_capital = 100000.0):
        """ Initialises the portfolio with bars and an event queue.

            Parameters:
            bars - The DataHandler object with current market data
            events - The Event Queue object
            start_date - The start date of portfolio
            initial_capital - Starting capital in USD
            """"

        self.bars = bars
        self.events = events
        self.symbol_list = self.bars.symbol_list
        self.start_date = start_date
        self.initial_capital = initial_capital

        self.all_positions = self.construct_all_positions()
        self.current_positions = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list])

        self.all_holdings = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()

    def construct_all_positions(self):

        d = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list])
        d['datetime'] = self.start_date
        return [d]

    def construct_all_holdings(self):

        d = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list])
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]

    def update_timeindex(self, event):
        """ Adds a new record to the positions matrix for the current
            market data bar. This reflects the previous bar, which is
            all current market data at this stage is know as (OLHCVI). """

        bars = {}
        for sym in self.symbol_list:
            bars[sym] = self.bars.get_latest_bars(sym, N = 1)

        # Update positions
        dp = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list])
        dp['datetime'] = bars[self.symbol_list[0]][0][1]

        for s in self.symbol_list:
            dp[s] = self.current_positions[s]

        # Append the current positions
        self.all_positions.append(dp)

        # Update holdings
        dh = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list])
        dh['datetime'] = bars[self.symbol_list[0]][0][1]
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] =self.current_holdings['commission']
        dh['total'] = self.current_holdings['cash']

        for s in self.symbol_list:
            # Approximation to the real value
            market_value = self.current_positions[s] * bars[s][0][5]
            dh[s] = market_value
            dh['total'] += market_value

        self.all_holdings.append(dh)

    def update_positions_from_fill(self, fill):
        # Check whether fill is a buy or a sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1

        self.current_positions[fill.symbol] += fill_dir * fill.quantity

    def update_holdings_from_fill(self, fill):
        # Check whether fill is a buy or a sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1

        # Update holdings list with new quantities
        fill_cost = self.bars.get_latest_bars(fill.symbol)[0][5] #Close Price
        cost = fill_dir * fill_cost * fill_quantity
        self.current_holdings[fill.symbol] += cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)

    def update_fill(self, event):
        if event.type == 'FILL':
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)

    def generate_naive_order(self, signal):
        """ Simply transacts an orderevent object as a constant quantity
            sizing of the signal object, without a risk management or position
            consideration."""

        order = None
        symbol = signal.symbol
        direction = signal.signal_type
        strength = signal.strength

        mkt_quantity = floor(100 * strength)
        cur_quantity = self.current_positions[symbol]
        order_type = 'MKT'

        if direction == 'LONG' and cur_quantity == 0:
            order = OrderEvent(symbol, order_type, mkt_quantity, 'BUY')
        if direction == 'SHORT' and cur_quantity == 0:
            order = OrderEvent(symbol, order_type, mkt_quantity, 'SELL')

        if direction == 'EXIT' and cur_quantity > 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'SELL')
        if direction == 'EXIT' and cur_quantity < 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'BUY')
        return order

    def update_signal(self, event):
        if event.type == 'SIGNAL':
            order_event = self.generate_naive_order(event)
            self.events.put(order_event)

    def create_equity_curve_dataframe(self):
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('datetime', inplace = True)
        curve['returns'] = curve['total'].pct_change()
        curve['equity_curve'] = (1.0 + curve['returns']).cumprod()
        self.equity_curve = curve

    def output_summary_stats(self):
        total_return = self.equity_curve['equity_curve'][-1]
        returns = self.equity_curve['returns']
        pnl = self.equity_curve['equity_curve']

        sharpe_ratio = create_sharpe_ratio(returns)
        max_dd, dd_duration = create_drawdowns(pnl)

        stats = [("Total Return", "%0.2f%%" ((total_return - 1.0) * 100.0)),
                ("Sharpe Ratio", "0.2f" % sharpe_ratio),
                ("Max Drawdown", "%0.2f%%" % (max_dd * 100.0)),
                ("Drawdown Duration", "%d" % dd_duration)]
        return stats
        
